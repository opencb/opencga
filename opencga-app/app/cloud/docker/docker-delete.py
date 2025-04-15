import docker
import os
import shutil
import subprocess
from getpass import getpass
from pathlib import Path

def delete_dockerhub_image(repo, version, username, password):
    """
    Delete a Docker image from Docker Hub.

    Args:
        repo (str): Docker Hub repository (e.g., "repo").
        version (str): Version (or tag) of the image to delete (e.g., "123456").
        username (str): Docker Hub username.
        password (str): Docker Hub password.
    """
    client = docker.from_env()

    # Authenticate (if credentials are provided)
    if username and password:
        client.login(
            username=username,
            password=password,
            registry="https://index.docker.io/v1/"
        )
        print("Logged in to Docker Hub")

    # Delete the image
    try:
        image_name = f"{username}/{repo}:{version}"
        client.images.remove(image_name, force=True)
        print(f"Deleted image: {image_name}")
    except APIError as e:
        print(f"Error deleting image: {e}")

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Delete image from Docker Hub"
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

    args = parser.parse_args()

    try:
        if not args.docker_repo:
            raise ValueError("Docker repository name is required.")
        if not args.docker_repo_version:
            raise ValueError("Docker repository version is required.")
        if not args.docker_username:
            raise ValueError("Docker Hub username is required.")
        if not args.docker_password:
            raise ValueError("Docker Hub password is required.")

        delete_dockerhub_image(args.docker_repo, args.docker_repo_version, args.docker_username, args.docker_password)

    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    import sys
    main()