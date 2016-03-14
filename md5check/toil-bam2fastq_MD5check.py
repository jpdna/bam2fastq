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
       job.addChildJobFn(runBAM2FASTQ, input_args, sample, cores=30, disk='600000000000')
      
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


    ######### fastq1 ############################################
    #############################################################

    myfilename = os.path.basename(mystring)

    with open(os.path.join(work_dir, myfilename), 'w') as f_out:    
       key.get_contents_to_file(f_out)
    
    ## Run MD5 on BAM
    
    mycommand = "md5sum " + os.path.join(work_dir, myfilename) + " > " + os.path.join(work_dir, myfilename + ".md5") 
    os.system(mycommand)

    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/' + myfilename + '.md5']
    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20', 
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, myfilename + '.md5')), 
                    's3://' + dest_bucket + '/sequence/md5/' + myfilename + '.md5']

    #Retry loop as s3am calls failed sporadically 
    retries = 2
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

    mysamplename = myfilename.replace(".raw.bam", "") 
    mysamplename = mysamplename.replace(".bam", "")


    # Run samtools in docker to make fastq
    sudo = input_args['sudo']
    parameters = ['bam2fq', '/data/mybam.bam']

    base_docker_call = 'docker run -dit --log-driver=none --entrypoint=/bin/bash -v {}:/data'.format(work_dir).split() 
    base_docker_call = base_docker_call + ['quay.io/ucsc_cgl/samtools:1.3--323f40b0c5dace98ced5521c6ae45a6c04df923b'] 
    containerName = subprocess.check_output(base_docker_call)
    formatcontainerName = containerName.rstrip()
    
    # would prefer to use subprocess_check_call() rather than os.system()
    # but had trouble escaping the nested quotes when using shell=True with subprocess
    #mycommand = "docker exec " + formatcontainerName + " sh -c 'samtools bam2fq /data/mybam.bam > /data/output_fastq.fastq'"
    
    #mycommand = "docker exec " + formatcontainerName + " sh -c 'samtools fastq -1 /data/" + mysamplename + "_1.fastq" + " -2 /data/" + mysamplename + "_2.fastq -0 /data/output_0.fastq /data/" + myfilename + "'" 
    mycommand = "docker exec " + formatcontainerName + " sh -c 'samtools fastq -1 /data/" + mysamplename + "_1.fastq" + " -2 /dev/null -0 /data/output_0.fastq /data/" + myfilename + "'" 

    os.system(mycommand)



    # Docker writes file with rw permission only by root at moment, fixed by chmod so toil can delete it when done
    mycommand2 = "docker exec " + formatcontainerName + " sh -c 'chmod 666 /data/" + mysamplename + "_1.fastq'"
    os.system(mycommand2)

    mycommand2 = "docker exec " + formatcontainerName + " sh -c 'chmod 666 /data/" + mysamplename + "_2.fastq'"
    os.system(mycommand2)

    mycommand2 = "docker exec " + formatcontainerName + " sh -c 'chmod 666 /data/output_0.fastq'"
    os.system(mycommand2)


    myclosecommand = "docker stop " + formatcontainerName
    os.system(myclosecommand)

    #delete temp bam filefile
    myrm_command = "rm " + os.path.join(work_dir, myfilename)
    os.system(myrm_command)
    
    
    #myfilename = myfilename.replace(".raw.bam", "")
    #myfilename = myfilename.replace(".bam", "")




    ################## Run MD5 on fastq1                                                                                                                                                                                                    
    mycommand = "md5sum " + os.path.join(work_dir, mysamplename + "_1.fastq") + " > " + os.path.join(work_dir, mysamplename + "_1.fastq.md5")
    os.system(mycommand)

    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/md5/unsorted/' + mysamplename + '_1.fastq.md5']
    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20',
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, mysamplename + '_1.fastq.md5')),
                    's3://' + dest_bucket + '/sequence/md5/unsorted/' + mysamplename + '_1.fastq.md5']

    #Retry loop as s3am calls failed sporadically                                                                                                                                                                           
    retries = 2
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



    ### Sorted fastq 1    
    myzip_command = "cat " + os.path.join(work_dir, mysamplename + '_1.fastq') + " | paste - - - - | sort -k1,1 -S 200G  -T " + os.path.join(work_dir, './') + " --parallel=30 | tr '\t' '\n' | md5sum > " + os.path.join(work_dir, mysamplename +  "_1.fastq.md5")


 
    os.system(myzip_command)

    #Upload to S3         
    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/md5/sorted/' + mysamplename + '_1.fastq.md5']

    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20', 
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, mysamplename + '_1.fastq.md5')), 
                    's3://' + dest_bucket + '/sequence/md5/sorted/' + mysamplename + '_1.fastq.md5']

    #Retry loop as s3am calls failed sporadically 
    retries = 20
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
    ############### End sorted fastq 1    




    ############### END Fastq1 #################################################
    #############################################################################

    #delete fastq_1
    myrm_command = "rm " + os.path.join(work_dir, mysamplename + '_1.fastq')
    os.system(myrm_command)




    ##################### Start fastq2 #################
    #######################################
    myfilename = os.path.basename(mystring)

    with open(os.path.join(work_dir, myfilename), 'w') as f_out:    
       key.get_contents_to_file(f_out)
    
    ## Run MD5 on BAM
    
    mycommand = "md5sum " + os.path.join(work_dir, myfilename) + " > " + os.path.join(work_dir, myfilename + ".md5") 
    os.system(mycommand)

    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/' + myfilename + '.md5']
    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20', 
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, myfilename + '.md5')), 
                    's3://' + dest_bucket + '/sequence/md5/' + myfilename + '.md5']

    #Retry loop as s3am calls failed sporadically 
    retries = 2
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

    mysamplename = myfilename.replace(".raw.bam", "") 
    mysamplename = mysamplename.replace(".bam", "")


    # Run samtools in docker to make fastq
    sudo = input_args['sudo']
    parameters = ['bam2fq', '/data/mybam.bam']

    base_docker_call = 'docker run -dit --log-driver=none --entrypoint=/bin/bash -v {}:/data'.format(work_dir).split() 
    base_docker_call = base_docker_call + ['quay.io/ucsc_cgl/samtools:1.3--323f40b0c5dace98ced5521c6ae45a6c04df923b'] 
    containerName = subprocess.check_output(base_docker_call)
    formatcontainerName = containerName.rstrip()
    
    # would prefer to use subprocess_check_call() rather than os.system()
    # but had trouble escaping the nested quotes when using shell=True with subprocess
    #mycommand = "docker exec " + formatcontainerName + " sh -c 'samtools bam2fq /data/mybam.bam > /data/output_fastq.fastq'"
    
    mycommand = "docker exec " + formatcontainerName + " sh -c 'samtools fastq -1 /dev/null" + " -2 /data/" + mysamplename + "_2.fastq -0 /data/output_0.fastq /data/" + myfilename + "'" 
    

    os.system(mycommand)



    # Docker writes file with rw permission only by root at moment, fixed by chmod so toil can delete it when done
    mycommand2 = "docker exec " + formatcontainerName + " sh -c 'chmod 666 /data/" + mysamplename + "_1.fastq'"
    os.system(mycommand2)

    mycommand2 = "docker exec " + formatcontainerName + " sh -c 'chmod 666 /data/" + mysamplename + "_2.fastq'"
    os.system(mycommand2)

    mycommand2 = "docker exec " + formatcontainerName + " sh -c 'chmod 666 /data/output_0.fastq'"
    os.system(mycommand2)


    myclosecommand = "docker stop " + formatcontainerName
    os.system(myclosecommand)

    #delete temp bam filefile
    myrm_command = "rm " + os.path.join(work_dir, myfilename)
    os.system(myrm_command)
    
    
    myfilename = myfilename.replace(".raw.bam", "")
    myfilename = myfilename.replace(".bam", "")







    ################## Run MD5 on fastq2                                                                                                                                                                                                    
    mycommand = "md5sum " + os.path.join(work_dir, mysamplename + "_2.fastq") + " > " + os.path.join(work_dir, mysamplename + "_2.fastq.md5")
    os.system(mycommand)

    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/md5/unsorted/' + mysamplename + '_2.fastq.md5']
    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20',
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, mysamplename + '_2.fastq.md5')),
                    's3://' + dest_bucket + '/sequence/md5/unsorted/' + mysamplename + '_2.fastq.md5']

    #Retry loop as s3am calls failed sporadically                                                                                                                                                                           
    retries = 2
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
    #############################################################################


    ### Sorted fastq 2    
    myzip_command = "cat " + os.path.join(work_dir, mysamplename + '_2.fastq') + " | paste - - - - | sort -k1,1 -S 200G  -T " + os.path.join(work_dir, './') + " --parallel=30 | tr '\t' '\n' | md5sum > " + os.path.join(work_dir, myfilename +  "_2.fastq.md5")


 
    os.system(myzip_command)

    #Upload to S3         
    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/sequence/md5/sorted/' + mysamplename + '_2.fastq.md5']

    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20', 
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, mysamplename + '_2.fastq.md5')), 
                    's3://' + dest_bucket + '/sequence/md5/sorted/' + mysamplename + '_2.fastq.md5']

    #Retry loop as s3am calls failed sporadically 
    retries = 20
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
    ############### End sorted fastq 2    



    

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

