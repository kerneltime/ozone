import argparse
import glob
import logging
import traceback
import os
import sys
import glob
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Validator(object):
    def __init__(self, in_dir):
        self._in_dir = in_dir
        self._share_classpath = os.path.join(self._in_dir, "hadoop-ozone/dist/target/ozone-*/share/ozone/classpath/*")

    def _snapshot_build(self):
        try:
            flag = False
            for file in glob.glob(self._share_classpath):
                with open(file, 'r') as f:
                    contents = f.read()
                if "SNAPSHOT" in contents:
                    flag = True
                    self._print_snapshot_jars(file)
                else:
                    continue

            if not flag:
                logging.info("No SNAPSHOT JARS present, validating JAR uniformity")
                self._jar_uniformity()
            else:
                logging.info("Build Failure: SNAPSHOT JARS present!")
                sys.exit(1)
                
        except Exception:
            logging.info("Exception in _snapshot_build {}".format(traceback.format_exc()))
            sys.exit(1)

    def _print_snapshot_jars(self, file):
        try:
            logging.info(f"SNAPSHOT JARS in {file}:")
            with open(file, 'r') as f:
                contents = f.read() 

            matches = re.findall("/[a-zA-Z0-9.-]*-SNAPSHOT.jar|/[a-zA-Z0-9.-]*-SNAPSHOT-shaded.jar", contents)
            matches = [i[1:] for i in matches]

            for i in matches:
                logging.info(f"Logging SNAPSHOT JAR: {i}")
        
        except Exception:
            logging.info("Exception in _print_snapshot_jars {}".format(traceback.format_exc()))
            sys.exit(1)

    def _jar_uniformity_util(self, contents):
        try:
            matches_ozone = re.findall("hdds-.+?.jar|ozone-.+?.jar|ratis-.+?.jar|hadoop-dependency-.+?.jar", contents)
            matches_others = re.findall("hadoop-[^dependency-][a-zA-Z0-9*.-]*[^-shaded].jar|ranger-[a-zA-Z0-9*.-]*[^-shaded].jar|gcs-[a-zA-Z0-9*.-]*[^-shaded].jar|solr-[a-zA-Z0-9*.-]*[^-shaded].jar", contents)
            matches_others_shaded = re.findall("hadoop-[^dependency-][a-zA-Z0-9*.-]*-shaded.jar|ranger-[a-zA-Z0-9*.-]*-shaded.jar|gcs-[a-zA-Z0-9*.-]*-shaded.jar|solr-[a-zA-Z0-9*.-]*-shaded.jar", contents)

            result1 = result2 = result3 = result4 = True

            if matches_ozone:
                result1 = all(re.search('-([a-zA-Z0-9*]*).jar', i).group(1) == re.search('-([a-zA-Z0-9*]*).jar', matches_ozone[0]).group(1) for i in matches_ozone)

            if matches_others:
                result2 = all(re.search('-([a-zA-Z0-9*]*).jar', i).group(1) == re.search('-([a-zA-Z0-9*]*).jar', matches_others[0]).group(1) for i in matches_others)

            if matches_others_shaded:
                result3 = all(re.search('-([a-zA-Z0-9*]*)-shaded.jar', i).group(1) == re.search('-([a-zA-Z0-9*]*)-shaded.jar', matches_others_shaded[0]).group(1) for i in matches_others_shaded)

            if (matches_others and matches_others_shaded):
                result4 = re.search('-([a-zA-Z0-9*]*).jar', matches_others[0]).group(1) == re.search('-([a-zA-Z0-9*]*)-shaded.jar', matches_others_shaded[0]).group(1)

            if result1 and result2 and result3 and result4:
                return True
            else:
                return False

        except Exception:
            logging.info("Exception in _jar_uniformity_util {}".format(traceback.format_exc()))
            sys.exit(1)
        
    def _jar_uniformity(self):
        try:
            flag = False
            for file in glob.glob(self._share_classpath):
                with open(file, 'r') as f:
                    contents = f.read()
                if not self._jar_uniformity_util(contents):
                    flag = True
                    break
                else:
                    continue

            if flag:
                logging.info("Build Failure: JAR build-specific details are not uniform!")
                sys.exit(1)
            else:
                logging.info("Post-build validation of classpaths is successful, proceeding to Ozone parcel generation")
                sys.exit(0)
                
        except Exception:
            logging.info("Exception in _jar_uniformity {}".format(traceback.format_exc()))
            sys.exit(1)

    def validate(self):
        self._snapshot_build()

def parse_args():
    parser = argparse.ArgumentParser(description="Post-build Validator Params")
    parser.add_argument('-id', '--input-directory', required=True, help="Directory containing input files.")

    if len(sys.argv[1:]) == 0:
            parser.print_help()
            sys.exit(1)

    args = parser.parse_args(sys.argv[1:])
    return args

def parcel_main(args):
    in_dir = os.path.abspath(os.path.expandvars(args.input_directory)) 

    if not os.path.exists(in_dir):
        logging.error("Input path %s does not exist", in_dir)
        sys.exit(1)

    if not os.path.isdir(in_dir):
        logging.error("Input path %s is not a directory", in_dir)
        sys.exit(1)

    logging.info("Using input directory %s", in_dir)

    v = Validator(in_dir)
    v.validate()

def main():
    args = parse_args()
    parcel_main(args)

if __name__ == "__main__":
    main()