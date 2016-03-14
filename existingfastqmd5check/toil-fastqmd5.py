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
       job.addChildJobFn(runFASTQSORT, input_args, sample, cores=12, disk='200000000000')
      
def runFASTQSORT(job, input_args, mystring):
    
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

    with open(os.path.join(work_dir, myfilename), 'w') as f_out:    
       key.get_contents_to_file(f_out)
     
    mysamplename = myfilename.replace(".gz", "")

    mycommand = "zcat " + os.path.join(work_dir, myfilename) + " | md5sum > " + os.path.join(work_dir, mysamplename + ".md5")
    os.system(mycommand)

    #Upload to S3         

    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/existingmd5check/unzipped/' + mysamplename + '.md5']

    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20', 
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, mysamplename + '.md5')), 
                    's3://' + dest_bucket + '/existingmd5check/unzipped/' + mysamplename + '.md5']


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


    ### zippedmd5
    mycommand = "md5sum " + os.path.join(work_dir, myfilename) + " > " +  os.path.join(work_dir, myfilename + ".md5")
    os.system(mycommand)

    #Upload to S3         

    s3am_command1= ['s3am', 'cancel', 's3://' + dest_bucket + '/existingmd5check/unzipped/' + myfilename]

    s3am_command2 = ['s3am', 'upload', '--upload-slots=20', '--download-slots=20', 
                    '--part-size=50000000', 'file://{}'.format(os.path.join(work_dir, myfilename + ".md5" )), 
                    's3://' + dest_bucket + '/existingmd5check/zipped/' + myfilename + ".md5"]



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
#python toil-bam2fastq.py aws:us-west-2:jp-test-700 --file_list filelist.txt  --src_bucket='jptemp1' --dest_bucket='cgl-sgdd-work' --batchSystem=mesos --mesosMaster=$(hostname -i):5050 --retryCount 5  --workDir /var/lib/toil/ > jptoil_run.log 2>&1 &

### Example filelist.txt content:
#s3://jptemp1/fastqtest/test1.fastq.gz
#s3://jptemp1/fastqtest/test2.fastq.gz

