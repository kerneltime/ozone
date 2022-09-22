import argparse
import glob
import json
import logging
import traceback
import os
import shutil
import subprocess
import sys
import yaml
import glob
import re
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CopyAndModifyData(object):
    def __init__(self, in_dir, scratch_dir, configs):
        self._in_dir = in_dir
        self._scratch_dir = scratch_dir
        self._bin = os.path.join(self._scratch_dir, "bin")
        self._hadoop_ozone = os.path.join(self._scratch_dir, "lib", "hadoop-ozone")
        self._lib_bin = os.path.join(self._hadoop_ozone, "bin")
        self._lib_libexec = os.path.join(self._hadoop_ozone, "libexec")
        self._lib_share_ozone = os.path.join(self._hadoop_ozone, "share", "ozone")
        self._share_classpath = os.path.join(self._lib_share_ozone, "classpath")
        self._share_lib = os.path.join(self._lib_share_ozone, "lib")
        self._docker_dir = os.path.abspath("cloudera/docker")
        self._configs = configs

    def _parcel_ozone_wrapper(self):
        os.makedirs(self._bin)
        parcel_ozone_wrapper = """\
        #!/bin/bash
        # Reference: http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
            SOURCE="${BASH_SOURCE[0]}"
            BIN_DIR="$( dirname "$SOURCE" )"
            while [ -h "$SOURCE" ]
            do
                SOURCE="$(readlink "$SOURCE")"
                [[ $SOURCE != /* ]] && SOURCE="$BIN_DIR/$SOURCE"
                BIN_DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd )"
            done
            BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
            LIB_DIR=$BIN_DIR/../lib

        IFS='.' read -ra HDP_VERSION_ARRAY <<< "${HDP_VERSION}"
        IFS="-" read -ra CHF_VERSION <<< "${HDP_VERSION_ARRAY[3]}"

        if [[ ( "${HDP_VERSION_ARRAY[0]}" == 7 ) && ( "${HDP_VERSION_ARRAY[1]}" == 1 ) && ( "${HDP_VERSION_ARRAY[2]}" -ge 8 ) && ( -n "${CHF_VERSION[0]}" ) && ( "${CHF_VERSION[0]}" -ge 0 ) ]]; then
            :
        else
            echo "Validation of CDH version has failed: The current CDH version ${HDP_VERSION} is older than the minimum expected CDH version 7.1.8.0 for using Ozone parcels."
            exit 1
        fi

        export PARCEL_OZONE_JARS_DIR=${PARCEL_OZONE_JARS_DIR:-$LIB_DIR/hadoop-ozone/share/ozone/lib}
        export OZONE_LIBEXEC_DIR=${OZONE_LIBEXEC_DIR:-$LIB_DIR/hadoop-ozone/libexec}
        export OZONE_HOME=${OZONE_HOME:-$LIB_DIR/hadoop-ozone}
        exec $LIB_DIR/hadoop-ozone/bin/parcel_ozone "$@"
        """

        outfile = os.path.join(self._bin, 'parcel_ozone')
        contents = parcel_ozone_wrapper

        try: 
            with open(outfile, "wt") as o:
                o.write(contents)
            subprocess.check_output(f"cd {self._bin} && chmod a+x parcel_ozone", shell=True)
        except Exception:
            logging.info("Exception in _parcel_ozone_wrapper {}".format(traceback.format_exc()))
            sys.exit(1)

    def _parcel_ozone_cli(self):
        os.makedirs(self._lib_bin)
        try: 
            inp_path = self._in_dir + "/hadoop-ozone/dist/target/ozone-*/bin/ozone"
            subprocess.check_output(f"cp {inp_path} {self._lib_bin}/parcel_ozone", shell=True)
            subprocess.check_output(f"cd {self._lib_bin} && chmod a+x parcel_ozone", shell=True)
        except Exception:
            logging.info("Exception in _parcel_ozone_cli {}".format(traceback.format_exc()))
            sys.exit(1)

    def _copy_jars(self):
        os.makedirs(self._share_lib)
        try:
            # copy hdds-jars
            for file in glob.glob(os.path.join(self._in_dir, "hadoop-ozone/dist/target/ozone-*/share/ozone/lib/hdds-*.jar")):
                logging.info(f"Copying {file} to {self._share_lib}")
                shutil.copy(file, self._share_lib)
            # copy ozone-jars
            for file in glob.glob(os.path.join(self._in_dir, "hadoop-ozone/dist/target/ozone-*/share/ozone/lib/ozone-*.jar")):
                logging.info(f"Copying {file} to {self._share_lib}")
                shutil.copy(file, self._share_lib)
            # copy ratis-jars
            for file in glob.glob(os.path.join(self._in_dir, "hadoop-ozone/dist/target/ozone-*/share/ozone/lib/ratis-*.jar")):
                logging.info(f"Copying {file} to {self._share_lib}")
                shutil.copy(file, self._share_lib)

            # copy ozone-filesystem jar
            for file in glob.glob(os.path.join(self._in_dir, "hadoop-ozone/dist/target/ozone-*/share/ozone/lib/ozone-filesystem-hadoop3-[0-9.]*-[a-zA-Z0-9]*.jar")):
                logging.info(f"Copying {file} to {self._hadoop_ozone}/ozone-filesystem-hadoop3.jar")
                shutil.copy(file, self._hadoop_ozone + "/ozone-filesystem-hadoop3.jar")
                shutil.copy(file, self._docker_dir)

            # copy additional_jars
            if self._configs['additional_jars']:
                additional_jars_list, exclude_jars_list = self._copy_jars_util(self._configs['additional_jars'], self._configs['exclude_jars'])
                for file in [jar for jar in additional_jars_list if jar not in exclude_jars_list]:
                    logging.info(f"Copying {file} to {self._share_lib}")
                    shutil.copy(file, self._share_lib)
        
        except Exception:
            logging.info("Exception in _copy_jars {}".format(traceback.format_exc()))
            sys.exit(1)

    def _copy_jars_util(self, additional_jars, exclude_jars):
        try:
            additional_jars_list, exclude_jars_list = [], []
            for files in additional_jars:
                for file in glob.glob(os.path.join(self._in_dir, f"hadoop-ozone/dist/target/ozone-*/share/ozone/lib/{files}")):
                    additional_jars_list.append(file)
        
            for files in exclude_jars:
                for file in glob.glob(os.path.join(self._in_dir, f"hadoop-ozone/dist/target/ozone-*/share/ozone/lib/{files}")):
                    exclude_jars_list.append(file)
        
            return additional_jars_list, exclude_jars_list

        except Exception:
            logging.info("Exception in _copy_jars_util {}".format(traceback.format_exc()))
            sys.exit(1)

    def _copy_classpaths(self):
        os.makedirs(self._share_classpath)
        try:
            for file in glob.glob(os.path.join(self._in_dir, "hadoop-ozone/dist/target/ozone-*/share/ozone/classpath/*")):
                shutil.copy(file, self._share_classpath)
            self._modify_classpaths()
        except Exception:
            logging.info("Exception in _copy_classpaths {}".format(traceback.format_exc()))
            sys.exit(1)

    def _modify_classpaths_util(self, contents, jars):
        try:
            env_var = "$HDDS_LIB_JARS_DIR/"
            for jar in jars:
                if f"{env_var}{jar}:" in contents:
                    contents = contents.replace(f"{env_var}{jar}:","")
                elif f":{env_var}{jar}" in contents:
                    contents = contents.replace(f":{env_var}{jar}","")
                elif f"={env_var}{jar}" in contents:
                    contents = contents.replace(f"={env_var}{jar}","=")
            return contents
        
        except Exception:
            logging.info("Exception in _modify_classpaths_util {}".format(traceback.format_exc()))
            sys.exit(1)

    def _modify_classpaths(self):
        try:
            for file in Path(self._share_classpath).glob("*"):
                with open(file, 'r') as f:
                    contents = f.read()

                matches = re.findall("hdds-.+?.jar|ozone-.+?.jar|ratis-.+?.jar", contents)

                # if additional_jars are specified under configs/parcel_config.yaml
                jars_strip = []
                if self._configs['additional_jars']:
                    additional_jars_list, exclude_jars_list = self._copy_jars_util(self._configs['additional_jars'], self._configs['exclude_jars'])
                    for jar in [jar for jar in additional_jars_list if jar not in exclude_jars_list]:
                        jars_strip.append(jar[jar.rindex("/")+1:] if '/' in jar else jar)
                
                jars_strip = [jar for jar in jars_strip if jar in contents]
                contents = self._modify_classpaths_util(contents, matches+jars_strip)
                res = re.sub("-([a-zA-Z0-9]*)-shaded.jar", "-*-shaded.jar", re.sub("-([a-zA-Z0-9]*[^shaded]).jar", "-*.jar", contents))
                res = re.sub("7.1.8.([0-9]*)\-\*\-shaded.jar", "7.1.8.*-*-shaded.jar", re.sub("7.1.8.([0-9]*)\-\*.jar", "7.1.8.*-*.jar", res))
                env_var = "$PARCEL_OZONE_JARS_DIR/"
                res = res[:10] + ''.join([f"{env_var}{i}:" for i in matches+jars_strip]) + res[10:]

                with open(file, 'w') as f:
                    f.write(res)
            
        except Exception:
            logging.info("Exception in _modify_classpaths {}".format(traceback.format_exc()))
            sys.exit(1) 

    def _update_libexec(self):
        os.makedirs(self._lib_libexec)
        try: 
            inp_path = self._in_dir + "/hadoop-ozone/dist/target/ozone-*/libexec"
            dest_path = str(self._lib_libexec)
            # copy ozone-functions.sh
            subprocess.check_output(f"cp {inp_path}/ozone-functions.sh {dest_path}", shell=True)
            # copy ozone-config.sh
            subprocess.check_output(f"cp {inp_path}/ozone-config.sh {dest_path}", shell=True)

            # copy shellprofile.d
            subprocess.check_output(f"cp -R {inp_path}/shellprofile.d {dest_path}", shell=True)

            self._modify_libexec()

        except Exception:
            logging.info("Exception in _update_libexec {}".format(traceback.format_exc()))
            sys.exit(1)

    def _modify_libexec(self):
        try: 
            file = os.path.join(self._lib_libexec, "ozone-functions.sh")
            with open(file, 'r') as f:
                filedata = f.read()
            to_replace = [
                ['export HDDS_LIB_JARS_DIR="${OZONE_HOME}/share/ozone/lib"', 'export HDDS_LIB_JARS_DIR=/opt/cloudera/parcels/CDH/lib/hadoop-ozone/share/ozone/lib'], 
                ['MAIN_ARTIFACT=$(find "$HDDS_LIB_JARS_DIR" -name "${OZONE_RUN_ARTIFACT_NAME}-*.jar")', 'MAIN_ARTIFACT=$(find "$PARCEL_OZONE_JARS_DIR" -name "${OZONE_RUN_ARTIFACT_NAME}-*.jar")'], 
                ['echo "ERROR: Component jar file $MAIN_ARTIFACT is missing from ${HDDS_LIB_JARS_DIR}"', 'echo "ERROR: Component jar file $MAIN_ARTIFACT is missing from ${PARCEL_OZONE_JARS_DIR}"']
            ]
            for text in to_replace:
                filedata = filedata.replace(text[0], text[1])
            with open(file, 'w') as f:
                f.write(filedata)
        except Exception:
            logging.info("Exception in _modify_libexec {}".format(traceback.format_exc()))
            sys.exit(1)

    def _docker_image_copy_files(self):
        try:
            dest_path_share_ozone_lib = self._docker_dir + "/share/ozone/lib"
            os.makedirs(dest_path_share_ozone_lib)

            self._docker_image_copy_files_util(dest_path_share_ozone_lib, "ozone-tools")

            dest_path_share_ozone_classpath = self._docker_dir + "/share/ozone/classpath"
            os.makedirs(dest_path_share_ozone_classpath)

            subprocess.check_output(f"cp {self._in_dir}/hadoop-ozone/dist/target/ozone-*/share/ozone/classpath/ozone-tools.classpath {dest_path_share_ozone_classpath}", shell=True)
            subprocess.check_output(f"cp {self._in_dir}/hadoop-ozone/dist/target/ozone-*/share/ozone/classpath/ozone-s3gateway.classpath {dest_path_share_ozone_classpath}", shell=True)

            inp_path_libexec = self._in_dir + "/hadoop-ozone/dist/target/ozone-*/libexec"
            dest_path_libexec = self._docker_dir + "/libexec"
            os.makedirs(dest_path_libexec)

            subprocess.check_output(f"cp {inp_path_libexec}/ozone-functions.sh {dest_path_libexec}", shell=True)
            subprocess.check_output(f"cp {inp_path_libexec}/ozone-config.sh {dest_path_libexec}", shell=True)
            subprocess.check_output(f"cp -R {inp_path_libexec}/shellprofile.d {dest_path_libexec}", shell=True)

            dest_path_ozone_cli = self._docker_dir + "/bin"
            os.makedirs(dest_path_ozone_cli)

            inp_path_ozone_cli = self._in_dir + "/hadoop-ozone/dist/target/ozone-*/bin/ozone"
            subprocess.check_output(f"cp {inp_path_ozone_cli} {dest_path_ozone_cli}", shell=True)
            subprocess.check_output(f"cd {dest_path_ozone_cli} && chmod a+x ozone", shell=True)

            inp_path_etc = self._in_dir + "/hadoop-ozone/dist/target/ozone-*/etc"
            subprocess.check_output(f"cp -R {inp_path_etc} {self._docker_dir}", shell=True)

            inp_path_compose = self._in_dir + "/hadoop-ozone/dist/target/ozone-*/compose/ozone"
            dest_path_compose = self._docker_dir + "/compose"
            os.makedirs(dest_path_compose)
            subprocess.check_output(f"cp -R {inp_path_compose} {dest_path_compose}", shell=True)

        except Exception:
            logging.info("Exception in _docker_image_copy_files {}".format(traceback.format_exc()))
            sys.exit(1)

    def _docker_image_copy_files_util(self, destination_path, classpath_name):
        try:
            for file in glob.glob(os.path.join(self._in_dir, f"hadoop-ozone/dist/target/ozone-*/share/ozone/lib/{classpath_name}-*.jar")):
                logging.info(f"Copying {file} to {destination_path}")
                shutil.copy(file, destination_path)

            for file in glob.glob(os.path.join(self._in_dir, "hadoop-ozone/dist/target/ozone-*/share/ozone/lib/*.jar")):
                for classpath_file in glob.glob(os.path.join(self._in_dir, f"hadoop-ozone/dist/target/ozone-*/share/ozone/classpath/{classpath_name}.classpath")):
                    with open(classpath_file, 'r') as f:
                        contents = f.read()
                    jar_name = file[file.rindex("/")+1:] if '/' in file else file
                    if jar_name in contents:
                        logging.info(f"Copying {file} to {destination_path}")
                        shutil.copy(file, destination_path)
        except Exception:
            logging.info("Exception in _docker_image_copy_files_util {}".format(traceback.format_exc()))
            sys.exit(1)

    def copy_and_modify(self):
        self._parcel_ozone_wrapper()
        self._parcel_ozone_cli()
        self._copy_jars()
        self._copy_classpaths()
        self._update_libexec()
        self._docker_image_copy_files()

class WriteMetadata(object):

    def __init__(self, scratch_dir, stack_version, gbn):
        self._scratch_dir = scratch_dir
        self._meta_dir = os.path.join(self._scratch_dir, "meta")
        self._gbn = gbn
        self._version = stack_version + "-1.ozone" + stack_version + ".p0." + self._gbn

    def _write_parcel_json(self):
        parcel = {
            "schema_version": 1,
            "name": "OZONE",
            "version": self._version,
            "extraVersionInfo": {
                "patchCount": 0,
            },
            "setActiveSymlink": True,
            "components": [
                {
                    "name": "ozone\n",
                    "pkg_release": self._gbn,
                    "pkg_version": "na\n",
                    "version": "na\n",
                }
            ],
            "packages": [
                {
                    "name": "ozone\n",
                    "version": "na\n"
                }
            ],
            "replaces": "OZONE",
            "scripts": {
                "defines": "parcel_ozone_env.sh",
            },
            "provides": [
                "cdh-plugin, ozone"
            ],
            "users": {
		        "hdfs": {
			        "extra_groups": [
                        "hadoop"
                    ],
			        "home": "/var/lib/hadoop-hdfs",
			        "longname": "Hadoop HDFS",
			        "shell": "/sbin/nologin"
		        }
	        },
            "groups": [
                "ozone"
            ]
        }

        out_file = os.path.join(self._meta_dir, "parcel.json")
        try:
            with open(out_file, "wt") as o:
                json.dump(parcel, o, sort_keys=True, indent=4, separators=(',', ': '))
        except Exception:
            logging.info("Exception in _write_parcel_json {}".format(traceback.format_exc()))
            sys.exit(1)


    def _write_parcel_ozone_env_sh(self):
        parcel_ozone_env_sh = """\
        #!/bin/bash
        PARCEL_OZONE_DIRNAME=${PARCEL_DIRNAME:-"OZONE-%s"}
        export CDH_OZONE_PARCEL_HOME=$PARCELS_ROOT/$PARCEL_OZONE_DIRNAME/lib/hadoop-ozone
        export CDH_OZONE_HOME=${CDH_OZONE_PARCEL_HOME}
        """ % (self._version)

        outfile = os.path.join(self._meta_dir, 'parcel_ozone_env.sh')
        contents = parcel_ozone_env_sh

        try: 
            with open(outfile, "wt") as o:
                o.write(contents)
        except Exception:
            logging.info("Exception in _write_parcel_ozone_env_sh {}".format(traceback.format_exc()))
            sys.exit(1)

    def _write_alternatives_json(self):
        binaries = [
            "parcel_ozone"
        ]

        alternatives = {}
        for b in binaries:
            alternatives[b] = {
                "destination": "/usr/bin/%s" % b,
                "source": "bin/%s" % b,
                "priority": 10,
                "isDirectory": False
            }
        
        alternatives['ozone-filesystem-hadoop3.jar'] = {
            "destination": "/var/lib/hadoop-hdfs/ozone-filesystem-hadoop3.jar",
            "source": "lib/hadoop-ozone/ozone-filesystem-hadoop3.jar",
            "priority": 10,
            "isDirectory": False
        }

        out_file = os.path.join(self._meta_dir, "alternatives.json")
        try: 
            with open(out_file, "wt") as o:
                json.dump(
                    alternatives,
                    o,
                    sort_keys=True,
                    indent=4,
                    separators=(',', ': '))
        except Exception:
            logging.info("Exception in _write_alternatives_json {}".format(traceback.format_exc()))
            sys.exit(1)

    def write_metadata(self):
        os.makedirs(self._meta_dir)

        self._write_parcel_json()
        self._write_parcel_ozone_env_sh()
        self._write_alternatives_json()

class Archive(object):
    def __init__(self, out_dir, linux_distro, stack_version, gbn):
        self._out_dir = out_dir
        self._scratch_dir = os.path.join(self._out_dir, "scratch_dir")
        self._gbn = gbn
        self._version = stack_version + "-1.ozone" + stack_version + ".p0." + self._gbn
        self._folder_name = "OZONE-" + self._version
        self._parcel_name = self._folder_name + "-" + linux_distro + ".parcel"

    def _archive_parcel(self):
        try:
            if not os.path.exists(f"{self._out_dir}/{self._folder_name}"):
                subprocess.check_output(f"mv {self._scratch_dir} {self._out_dir}/{self._folder_name}", shell=True)
            subprocess.check_output(f"cd {self._out_dir} && tar -czf {self._parcel_name} {self._folder_name}", shell=True)
            sha1code = self._generate_checksum()
            self._write_manifest_json(sha1code)
            logging.info(f"SUCCESS! {self._parcel_name} created at {self._out_dir}")
        except Exception:
            logging.info("Exception in _archive_parcel {}".format(traceback.format_exc()))
            sys.exit(1)

    def _generate_checksum(self):
        try:
            subprocess.check_output(f"cd {self._out_dir} && echo $(shasum "+self._parcel_name+" | awk '{ print $1 }') > "+self._parcel_name+".sha1", shell=True)
            subprocess.check_output(f"cd {self._out_dir} && echo $(shasum -a 256 "+self._parcel_name+" | awk '{ print $1 }') > "+self._parcel_name+".sha256", shell=True)
            with open(f"{self._out_dir}/{self._parcel_name}.sha1", 'r') as f:
                sha1code = f.read()
            return sha1code
        except Exception:
            logging.info("Exception in _generate_checksum {}".format(traceback.format_exc()))
            sys.exit(1)

    def _write_manifest_json(self, sha1code):
        distro = {
            "components": [
                {
                    "pkg_version": "na\n",
                    "pkg_release": self._gbn,
                    "name": "ozone\n",
                    "version": "na\n"
                }
            ],
            "description": "Ozone Runtime",
            "displayName": "Ozone Runtime",
            "hash": sha1code.strip(),
            "parcelName": self._parcel_name,
            "replaces": "OZONE"
        }
        manifest = {
            "lastUpdated": int(time.time()),
            "parcels": [
                distro
            ]}
        out_file = os.path.join(self._out_dir, "manifest.json")
        try:
            if (os.path.exists(out_file) and os.path.isfile(out_file)):
                manifest = json.loads(open(out_file, "r").read())
                manifest["parcels"].append(distro)
            with open(out_file, "wt") as o:
                json.dump(manifest, o, sort_keys=True, indent=4, separators=(',', ': '))
        except Exception:
            logging.info("Exception in _write_manifest_json {}".format(traceback.format_exc()))
            sys.exit(1)

    def archive_parcel_wrapper(self):
        self._archive_parcel()

def parse_args():
    parser = argparse.ArgumentParser(description="Builds parcel-ozone")

    parser.add_argument('-id', '--input-directory', required=True, help="Directory containing input files.")
    parser.add_argument('-od', '--output-directory', required=True, help="Directory to store output files. Will be created if not already.")
    parser.add_argument('-sv', '--stack-version', dest="stack_version", required=True, help="Stack Version")
    parser.add_argument('-gb', dest="gbn", required=True, help="GBN")
    parser.add_argument("-pc", "--parcel-config", dest="parcel_config", default="cloudera/configs/parcel_config.yaml", nargs="?", const="cloudera/configs/parcel_config.yaml", help="Parcel related config for setting up the parcel")
    parser.add_argument('-fc','--force-clean', dest='force_clean', default=0, nargs="?", help="Whether to delete contents of the output/scratch directory beforehand.")

    if len(sys.argv[1:]) == 0:
            parser.print_help()
            sys.exit(1)

    args = parser.parse_args(sys.argv[1:])
    return args 

def get_parcel_configs(args):
    parcel_config_path = os.path.abspath(os.path.expandvars(args.parcel_config)) 
    logging.info("Using config file %s", parcel_config_path)
    with open('{}'.format(parcel_config_path), encoding='utf8') as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            logging.info(exc)
    return config

def parcel_main(args, configs):
    in_dir = os.path.abspath(os.path.expandvars(args.input_directory)) 
    out_dir = os.path.abspath(os.path.expandvars(args.output_directory))
    scratch_dir = os.path.join(out_dir, "scratch_dir")

    if not os.path.exists(in_dir):
        logging.error("Input path %s does not exist", in_dir)
        sys.exit(1)

    if not os.path.isdir(in_dir):
        logging.error("Input path %s is not a directory", in_dir)
        sys.exit(1)

    if (int(args.force_clean) and os.path.exists(out_dir) and os.path.isdir(out_dir)):
        logging.info("--force-clean specified, deleting output directory %s", out_dir)
        shutil.rmtree(out_dir)

    logging.info("Creating scratch directory %s", scratch_dir)
    os.makedirs(scratch_dir)

    logging.info("Using input directory %s", in_dir)
    logging.info("Using output directory %s", out_dir)
    logging.info("Using scratch directory %s", scratch_dir)

    stack_version = str(args.stack_version)
    gbn = str(args.gbn)

    logging.info("Stack Version %s", stack_version)
    logging.info("GBN %s", gbn)

    c = CopyAndModifyData(in_dir, scratch_dir, configs)
    c.copy_and_modify()

    m = WriteMetadata(scratch_dir, stack_version, gbn)
    m.write_metadata()

    for linux_distro in configs['linux_distro']:
        a = Archive(out_dir, linux_distro, stack_version, gbn)
        a.archive_parcel_wrapper()

def main():
    args = parse_args()
    configs = get_parcel_configs(args)
    parcel_main(args, configs)

if __name__ == "__main__":
    main()