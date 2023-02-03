from prefect.filesystems import GitHub

github_block = GitHub.load("github-zoomcamp-hw")
print (github_block)