**Steps to Run**

Using **skaffold** ( if **kubectl** is setup)

Navigate to the directory

    skaffold dev
If skafflod is not installed

Build the docker container, push to a repo
fill up the env variables and the docker repo in all the  files  present in the the k8s_manifests directory

     kubectl apply -f <all_the_files>
