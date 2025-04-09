import shutil
import os
import subprocess
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

def build_docker_image(dockerfile_path: str, image_name: str = "regenie-walker"):
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
    print(f"Building Docker image '{' '.join(build_cmd)}'...")
    result = subprocess.run(
        build_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode != 0:
        error_msg = f"Build failed with error:\n{result.stderr}"
        raise RuntimeError(error_msg)

    print(f"Successfully built image '{image_name}'")
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
        "--image-name",
        default="regine-walker",
        help="Name for the Docker image"
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Only generate Dockerfile, don't build"
    )

    args = parser.parse_args()

    try:
        output_path = Path(args.output_dockerfile).parent
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
                    print(f"Failed to delete {file_path}. Reason: {e}")
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

        print(f"Generated Dockerfile at {dockerfile_path}")

        # Build image unless --no-build specified
        if not args.no_build:
            image_name = build_docker_image(dockerfile_path, args.image_name)
            print(f"\nRun the image with:")
            print(f"docker run -it --rm {image_name}")

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    import sys
    main()