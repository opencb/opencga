import shutil
import os
import subprocess
import logging
from getpass import getpass
from pathlib import Path

def generate_dockerfile(source_image: str, output_path: str):
    """
    Generate a Dockerfile that:
    1. Uses the source image (it should be opencb/opencga-regenie:version
    2. Copies files from a directory (known at Dockerfile creation time)

    Args:
        output_path: Where to save the generated Dockerfile
    """

    # Dockerfile content
    dockerfile_content = f"""# Dynamically generated Dockerfile
FROM {source_image}

# Create working directory
WORKDIR /regenie_walker/python

# Copy files from host (known at Dockerfile creation time)
COPY ./regenie_walker /regenie_walker
"""

    # Write Dockerfile
    with open(output_path, 'w') as f:
        f.write(dockerfile_content)

    return output_path


def build_docker_image(dockerfile_path: str, image_name: str):
    """
    Build Docker image from the generated Dockerfile.
    """
    if not os.path.exists(dockerfile_path):
        raise FileNotFoundError(f"Dockerfile not found at {dockerfile_path}")

    output_path = Path(dockerfile_path).parent

    build_cmd = [
        "docker", "build",
        "-t", image_name,
        "-f", dockerfile_path,
        str(output_path)
    ]
    logging.info(f"Building Docker image '{' '.join(build_cmd)}'...")
    result = subprocess.run(
        build_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode != 0:
        error_msg = f"Build failed with error: {result.stderr}"
        logging.error(error_msg);
        raise RuntimeError(error_msg)

    logging.info(f"Successfully built image '{image_name}'")
    return image_name

def copy_files(src_folder: str, dest_folder: str):
    # Copy all files from source to destination
    for filename in os.listdir(src_folder):
        src_file = os.path.join(src_folder, filename)
        dest_file = os.path.join(dest_folder, filename)
        if os.path.isfile(src_file):  # Only copy files (not subfolders)
            shutil.copy2(src_file, dest_file)  # Preserves metadata (timestamps, etc.)
        elif os.path.isdir(src_file):  # If you want to copy subfolders recursively
            shutil.copytree(src_file, dest_file)  # Recursive copy


def push_to_dockerhub(local_image_name, repo, version, username, password):
    """
    Pushes a locally built Docker image to Docker Hub with the specified repository and tag.
    """
    full_image_name = f"{username}/{repo}:{version}"

    logging.info(f"Tagging local image '{local_image_name}' as '{full_image_name}'...")
    tag_cmd = [
        "docker", "tag",
        local_image_name,
        full_image_name
    ]
    result_tag = subprocess.run(
        tag_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result_tag.returncode != 0:
        error_msg = f"Failed to tag image '{local_image_name}' as '{full_image_name}': {result_tag.stderr}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)

    logging.info(f"Successfully tagged '{local_image_name}' as '{full_image_name}'.")
    logging.info(f"Logging into Docker Hub as '{username}'...")
    login_cmd = [
        "docker", "login",
        "-u", username,
        "--password-stdin"
    ]
    process = subprocess.Popen(login_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate(input=password)

    if process.returncode != 0:
        error_msg = f"Docker login failed: {stderr}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)

    logging.info(f"Successfully logged into Docker Hub.")
    logging.info(f"Pushing Docker image '{full_image_name}'...")
    push_cmd = [
        "docker", "push",
        full_image_name
    ]
    result_push = subprocess.run(
        push_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result_push.returncode != 0:
        error_msg = f"Push failed for image '{full_image_name}' with error: {result_push.stderr}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)

    logging.info(f"Successfully pushed image '{full_image_name}' to Docker Hub.")
    return full_image_name

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate Dockerfile for regenie walker and build image"
    )
    parser.add_argument(
        "--step1-path",
        required=True,
        help="Regenie step1 results path"
    )
    parser.add_argument(
        "--python-path",
        required=True,
        help="OpenCGA Python utils path"
    )
    parser.add_argument(
        "--pheno-file",
        required=True,
        help="Phenotype path"
    )
    parser.add_argument(
        "--covar-file",
        help="Covariables path"
    )
    parser.add_argument(
        "--output-dockerfile",
        default="Dockerfile",
        help="Output Dockerfile path"
    )
    parser.add_argument(
        "--source-image-name",
        required=True,
        help="Name for the Docker image"
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Only generate Dockerfile, don't build"
    )
    parser.add_argument(
        "--docker-username",
        default=None,
        help="Docker Hub username"
    )
    parser.add_argument(
        "--docker-password",
        default=None,
        help="Docker Hub password"
    )
    parser.add_argument(
        "--docker-repo",
        default=None,
        help="Docker Hub repository name"
    )
    parser.add_argument(
        "--docker-repo-version",
        default=None,
        help="Docker Hub repository version"
    )

    args = parser.parse_args()

    try:
        output_path = Path(args.output_dockerfile).parent

        # Setup logging
        logging.basicConfig(
            filename=f"{output_path}/regenie-docker-build.log",
            level=logging.INFO,
            format="%(asctime)s - %(message)s",
        )

        regenie_walker_path = os.path.join(output_path , "regenie_walker")
        if os.path.exists(regenie_walker_path):
            # Empty the folder (remove all contents)
            for filename in os.listdir(regenie_walker_path):
                file_path = os.path.join(regenie_walker_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)  # Delete file/link
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)  # Delete subfolder
                except Exception as e:
                    logging.error(f"Failed to delete {file_path}. Reason: {e}")
                    sys.exit(1)
        else:
            # Create the destination folder if it doesn't exist
            os.makedirs(regenie_walker_path, exist_ok=True)

        # Copy step1 results
        step1_path = os.path.join(regenie_walker_path, "step1")
        os.makedirs(step1_path, exist_ok=True)
        copy_files(args.step1_path, step1_path)
        # Read the file and replace paths
        step1_list_path = os.path.join(step1_path, "step1_pred.list")
        with open(step1_list_path, 'r') as f:
            lines = f.readlines()

        updated_lines = []
        for line in lines:
            if line.strip():  # Skip empty lines
                parts = line.split()
                if len(parts) == 2:  # Ensure format is correct (e.g., "PHENO /path/file")
                    key, old_path = parts
                    filename = os.path.basename(old_path)  # Extract filename (e.g., "step1_1.loco")
                    new_path = f"/regenie_walker/step1/{filename}"  # New path
                    updated_lines.append(f"{key} {new_path}\n")

        # Write the updated content back
        with open(step1_list_path, 'w') as f:
            f.writelines(updated_lines)

        # Copy OpenCGA Python scripts
        python_path = os.path.join(regenie_walker_path, "python")
        os.makedirs(python_path, exist_ok=True)
        copy_files(args.python_path, python_path)

        # Copy phenotype file
        shutil.copy2(args.pheno_file, regenie_walker_path)

        # Copy covariables file
        if hasattr(args, 'covar_file') and args.covar_file is not None:
            if os.path.exists(args.covar_file):
                shutil.copy2(args.covar_file, regenie_walker_path)

        # Generate Dockerfile
        dockerfile_path = generate_dockerfile(args.source_image_name, args.output_dockerfile)

        logging.info(f"Generated Dockerfile at {dockerfile_path}")

        # Build image unless --no-build specified
        if not args.no_build:
            image_name = f"{args.docker_username}/{args.docker_repo}:{args.docker_repo_version}"
            logging.info(f"Building docker image: {image_name}")
            build_docker_image(dockerfile_path, image_name)
            logging.info(f"Run the image with: docker run -it --rm {image_name}")
            if args.docker_repo and args.docker_repo_version and args.docker_username and args.docker_password:
                logging.info(f"Pushing docker image: {image_name}")
                push_to_dockerhub(
                    image_name,
                    args.docker_repo,
                    args.docker_repo_version,
                    args.docker_username,
                    args.docker_password
                )

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    import sys
    main()