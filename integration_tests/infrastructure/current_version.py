
registry, repository, version = None, None, None

with open("../../README.md") as readme:
    for line in readme.readlines():
        if line.startswith("* Registry: "):
            registry = line.replace("* Registry: ", "").strip()
        if line.startswith("* Repository: "):
            repository = line.replace("* Repository: ", "").strip()
        if line.startswith("* Version: "):
            version = line.replace("* Version: ", "").strip()

        if registry and repository and version:
            break

docker_image_url = f"{registry}/{repository}:{version}"
