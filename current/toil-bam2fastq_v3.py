import argparse
import glob
import hashlib
import os
import shutil
import subprocess
import tarfile
import time
from toil.job import Job
import boto
import time


### DEPENDENCY: pigz multi-threaded gzip compatibly utility must be available on worker nodes


def build_parser():
    parser = argparse.ArgumentParser(description=main.__doc__, formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-f', '--file_list', default=None, required=True, help='Path to a file with list of S3 file ' 
                                                                              ' URLS to BAM files to convert to fastq')

    parser.add_argument('-u', '--sudo', dest='sudo', action='store_true', help='Docker usually needs sudo to execute '
                                                                               'locally, but not''when running Mesos '
                                                                               'or when a member of a Docker group.')

    parser.add_argument('-s', '--src_bucket', default=None, required=True, help='source S3 bucket name')

    parser.add_argument('-d', '--dest_bucket', default=None, required=True, help='desintation S3 bucket name')

    return parser

def start_batch(job, input_args):
    """
    This start processing the start_batch
    """

    mysampleslist = input_args['mysampleslist']

    for sample in mysampleslist:
       job.addChildJobFn(runBAM2FASTQ, input_args, sample, cores=15, disk='600000000000')
      


def s3am_upload(inputlocalfile, s3_key):
    s3am_command1 = ['s3am', 'cancel', s3_key]
    s3am_command2 =  ['s3am', 'upload', '--upload-slots=20', '--download-slots=20',
                    '--part-size=50000000', 'file://{}'.format(inputlocalfile),
                    s3_key]
        #Retry loop as s3am calls failed sporadically                                                                                                                                                                                                         
    retries = 1
    for i in range(retries):
      try:
        print "trying..."
        #subprocess.check_call(s3am_command1)
        #print s3am_command1
        #print s3am_command2
        time.sleep(5)
        #subprocess.check_call(s3am_command2)
      except:
          if i < retries:
            time.sleep(5)
            continue
          else:
            raise
      break


      
def runBAM2FASTQ(job, input_args, mystring):
    
    src_bucket = input_args['src_bucket']
    dest_bucket = input_args['dest_bucket']

    #myfile_no_bucket = mystring.replace("s3://cgl-sgdd-reorg/", "")
    
    myfile_no_bucket = mystring.replace("s3://" + src_bucket + "/", "")
    mydir_no_bucket = os.path.dirname('/' + myfile_no_bucket)

    s3 = boto.connect_s3()
    
    bucket = s3.lookup(src_bucket)

    key = bucket.lookup(myfile_no_bucket)
    work_dir = job.fileStore.getLocalTempDir()
    myfilename = os.path.basename(mystring)

    mysamplename = myfilename
    
    mysamplename = mysamplename.replace(".raw.bam", "")
    mysamplename = mysamplename.replace(".bam", "")    

    with open(os.path.join(work_dir, 'mybam.bam'), 'w') as f_out:    
       key.get_contents_to_file(f_out)
    
    # Run samtools in docker to make fastq
    sudo = input_args['sudo']
    parameters = ['bam2fq', '/data/mybam.bam']

    base_docker_call = 'docker run -dit --log-driver=none --entrypoint=/bin/bash -v {}:/data'.format(work_dir).split() 
    base_docker_call = base_docker_call + ['quay.io/ucsc_cgl/samtools:1.3--323f40b0c5dace98ced5521c6ae45a6c04df923b'] 

    try:
        containerName = subprocess.check_output(base_docker_call)
        containerName = containerName.rstrip()
        
        #mycommand = "docker exec " + formatcontainerName + " sh -c 'samtools fastq -1 /data/output_1.fastq -2 /data/output_2.fastq -0 /data/output_0.fastq /data/mybam.bam'"
        docker_exec_command = ['docker', 'exec', containerName, 'samtools', 'fastq', '-1', '/data/' + mysamplename + '_1.fastq', '-2', '/data/' + mysamplename + '_2.fastq', '-0', '/data/' + mysamplename + '_0.fastq', '/data/mybam.bam']

        subprocess.check_call(docker_exec_command)

    

        # Docker writes file with rw permission only by root at moment, fixed by chmod so toil can delete it when done
        docker_exec_command = ['docker', 'exec', containerName, 'chmod', '666', '/data/' + mysamplename + '_1.fastq']
        subprocess.check_call(docker_exec_command)

        docker_exec_command = ['docker', 'exec', containerName, 'chmod', '666', '/data/' + mysamplename + '_2.fastq']
        subprocess.check_call(docker_exec_command)

        docker_exec_command = ['docker', 'exec', containerName, 'chmod', '666', '/data/' + mysamplename + '_0.fastq']
        subprocess.check_call(docker_exec_command)

        docker_stop_cmd = ['docker', 'stop', containerName]
        subprocess.check_call(docker_stop_cmd)

        #delete temp bam filefile
        myrm_command = ['rm', os.path.join(work_dir, 'mybam.bam')]
        subprocess.check_call(myrm_command)
        
        myfilename = myfilename.replace(".raw.bam", "")
        myfilename = myfilename.replace(".bam", "")

        #zip the fastq file
        #pigz must be available on worker machine, gzip could be used instead, but slower
        # todo:make a pigz docker image available on quay.io to remove dependency

        with open(os.path.join(work_dir, myfilename +  "_1.fastq.md5"), "w") as outfile:
          md5_command = ['md5sum', os.path.join(work_dir, mysamplename + '_1.fastq')]
          subprocess.check_call(md5_command, stdout=outfile)

        with open(os.path.join(work_dir, myfilename +  "_2.fastq.md5"), "w") as outfile:
          md5_command = ['md5sum', os.path.join(work_dir, mysamplename + '_2.fastq')]
          subprocess.check_call(md5_command, stdout=outfile)

        #with open(os.path.join(work_dir, myfilename +  "_0.fastq.md5"), "w") as outfile:
        #  md5_command = ['md5sum', os.path.join(work_dir, mysamplename + '_0.fastq')]
        #  subprocess.check_call(md5_command, stdout=outfile)



        with open(os.path.join(work_dir, myfilename +  "_1.fastq.gz"), "w") as outfile:
          zip_command = ['pigz', '-c', '-p', '15', os.path.join(work_dir, mysamplename + '_1.fastq')]
          subprocess.check_call(zip_command, stdout=outfile)


        with open(os.path.join(work_dir, myfilename +  "_2.fastq.gz"), "w") as outfile:
          zip_command = ['pigz', '-c', '-p', '15', os.path.join(work_dir, mysamplename + '_2.fastq')]
          subprocess.check_call(zip_command, stdout=outfile)


        #with open(os.path.join(work_dir, myfilename +  "_0.fastq.gz"), "w") as outfile:
         # zip_command = ['pigz', '-c', '-p', '15', os.path.join(work_dir, mysamplename + '_0.fastq')]
         # subprocess.check_call(zip_command, stdout=outfile)


        with open(os.path.join(work_dir, myfilename +  "_1.fastq.gz.md5"), "w") as outfile:
          md5_command = ['md5sum', os.path.join(work_dir, mysamplename + '_1.fastq.gz')]
          subprocess.check_call(md5_command, stdout=outfile)

        with open(os.path.join(work_dir, myfilename +  "_2.fastq.gz.md5"), "w") as outfile:
          md5_command = ['md5sum', os.path.join(work_dir, mysamplename + '_2.fastq.gz')]
          subprocess.check_call(md5_command, stdout=outfile)




        #Upload to S3         
        ### for unknown reason the s3am_upload function will not work when run in toil job - so instead code is cut and pasted :(  
        """
        inputlocalfile = os.path.join(work_dir, myfilename + '_1.fastq.gz')
        s3_key = 's3://' + dest_bucket + '/sequence/' + myfilename + '_1.fastq.gz'
        s3am_upload(inputlocalfile, s3_key)

        inputlocalfile = os.path.join(work_dir, myfilename + '_2.fastq.gz')
        s3_key = 's3://' + dest_bucket + '/sequence/' + myfilename + '_2.fastq.gz'
        s3am_upload(inputlocalfile, s3_key)

        inputlocalfile = os.path.join(work_dir, myfilename + '_0.fastq.gz')
        s3_key = 's3://' + dest_bucket + '/sequence/' + myfilename + '_0.fastq.gz'
        s3am_upload(inputlocalfile, s3_key)
        """ 

    except subprocess.CalledProcessError:
        raise RuntimeError('docker command returned a non-zero exist status.  Check error logs.')
    except OSError:
        raise RuntimeError('docker not found on system. Install on all nodes.')



    #########################################################
    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/' + myfilename + '_1.fastq.gz']
    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20', 
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, myfilename + '_1.fastq.gz')), 
                    's3://' + dest_bucket + '/sequence/' + myfilename + '_1.fastq.gz']
    #Retry loop as s3am calls failed sporadically 
    retries = 3
    for i in range(retries):
      try:
        subprocess.check_call(s3am_command1)
        time.sleep(5)
        subprocess.check_call(s3am_command2)
      except:
          if i < retries:
            time.sleep(5)
            continue
          else:
            raise
      break
    ######################################################### 
    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/' + myfilename + '_1.fastq.md5']
    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20',
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, myfilename + '_1.fastq.md5')),
                    's3://' + dest_bucket + '/sequence/' + myfilename + '_1.fastq.md5']
    #Retry loop as s3am calls failed sporadically                                                                                                                                                                                                            
    retries = 3
    for i in range(retries):
      try:
        subprocess.check_call(s3am_command1)
        time.sleep(5)
        subprocess.check_call(s3am_command2)
      except:
          if i < retries:
            time.sleep(5)
            continue
          else:
            raise
      break
    ###############################################


    #########################################################                                                                                                                                                                                                 
    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/' + myfilename + '_1.fastq.gz.md5']
    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20',
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, myfilename + '_1.fastq.gz.md5')),
                    's3://' + dest_bucket + '/sequence/' + myfilename + '_1.fastq.gz.md5']
    #Retry loop as s3am calls failed sporadically                                                                                                                                                                                                            
    retries = 3
    for i in range(retries):
      try:
        subprocess.check_call(s3am_command1)
        time.sleep(5)
        subprocess.check_call(s3am_command2)
      except:
          if i < retries:
            time.sleep(5)
            continue
          else:
            raise
      break
    ###############################################  







 #########################################################                                                                                                                                                                                                 
    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/' + myfilename + '_2.fastq.gz']
    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20',
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, myfilename + '_2.fastq.gz')),
                    's3://' + dest_bucket + '/sequence/' + myfilename + '_2.fastq.gz']
    #Retry loop as s3am calls failed sporadically                                                                                                                                                                                                             
    retries = 3
    for i in range(retries):
      try:
        subprocess.check_call(s3am_command1)
        time.sleep(5)
        subprocess.check_call(s3am_command2)
      except:
          if i < retries:
            time.sleep(5)
            continue
          else:
            raise
      break
    #########################################################                                                                                                                                                                                                 
    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/' + myfilename + '_2.fastq.md5']
    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20',
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, myfilename + '_2.fastq.md5')),
                    's3://' + dest_bucket + '/sequence/' + myfilename + '_2.fastq.md5']
    #Retry loop as s3am calls failed sporadically                                                                                                                                                                                                            
    retries = 3
    for i in range(retries):
      try:
        subprocess.check_call(s3am_command1)
        time.sleep(5)
        subprocess.check_call(s3am_command2)
      except:
          if i < retries:
            time.sleep(5)
            continue
          else:
            raise
      break
    ############################################### 

   #########################################################                                                                                                                                                                                                \
                                                                                                                                                                                                                                                              
    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/' + myfilename + '_2.fastq.gz.md5']
    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20',
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, myfilename + '_2.fastq.gz.md5')),
                    's3://' + dest_bucket + '/sequence/' + myfilename + '_2.fastq.gz.md5']
    #Retry loop as s3am calls failed sporadically                                                                                                                                                                                                            
    retries = 3
    for i in range(retries):
      try:
        subprocess.check_call(s3am_command1)
        time.sleep(5)
        subprocess.check_call(s3am_command2)
      except:
          if i < retries:
            time.sleep(5)
            continue
          else:
            raise
      break
    ###############################################  



 
    return 



def main():
    """
    Run S3am verify on input list of files in S3
    """
    # Define Parser object and add to toil
    parser = build_parser()

    Job.Runner.addToilOptions(parser)
    args = parser.parse_args()

    # Store inputs from argparse
    inputs = {'filelist': args.file_list, 'sudo': args.sudo, 'src_bucket': args.src_bucket, 'dest_bucket': args.dest_bucket }
    

    mysamples = []
    with open(inputs['filelist'], 'r') as f:
      for line in f.readlines():
         mysamples.append(line.strip())

    inputs['mysampleslist'] = mysamples

    # Start Pipeline
    args.logLevel = "INFO"
    Job.Runner.startToil(Job.wrapJobFn(start_batch, inputs), args)

if __name__ == '__main__':
    main()

#example usage
#python toil-bam2fastq.py aws:us-west-2:jp-test-700 --file_list filelist.txt  --src_bucket='cgl-sgdd-reorg' --dest_bucket='cgl-sgdd-work' --batchSystem=mesos --mesosMaster=$(hostname -i):5050 --retryCount 5  --workDir /var/lib/toil/ > jptoil_run.log 2>&1 &

