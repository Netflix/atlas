## Introduction

The [Zero to Docker](https://github.com/Netflix-Skunkworks/zerotodocker) project tracks the Dockerfiles used to create official Netflix OSS containers on [Docker Hub](https://registry.hub.docker.com/repos/netflixoss/).

The [Atlas container](https://github.com/Netflix-Skunkworks/zerotodocker/wiki/Atlas) provides some fake in-memory data for testing the endpoints and graphing capabilities.

```
# build environment testing
docker run -it netflixoss/java:8 /bin/bash

# build command
docker build -t netflixoss/atlas $DOCKERFILE_HOME

# interactive run command
docker run \
  --name atlas \
  -p 7101:7101 \
  netflixoss/atlas:1.4.1

# detached run command
docker run -d \
  --name atlas \
  -p 7101:7101 \
  netflixoss/atlas:1.4.1

# view port mappings
docker port atlas

# connect interactive bash shell to a container
docker exec -it atlas bash

# explore running containers
docker ps -a
docker start $CONTAINER_NAME_OR_ID
docker stop $CONTAINER_NAME_OR_ID
docker rm $CONTAINER_NAME_OR_ID

# explore container images
docker images
docker pull $IMAGE_NAME_OR_ID
docker rmi $IMAGE_NAME_OR_ID
docker rmi -f $IMAGE_NAME_OR_ID
```

The Atlas container will run a standalone Java process on port 7101 and this will be mapped to port 0.0.0.0:7101 on the host machine running the container.  Using this mapping, you can test Atlas with a curl command to one of the endpoints.  See [[Graph]] and [[Tags]] for more details on endpoints.

```
curl -s http://localhost:7101/api/v1/tags
```

## Testing with Vagrant

The [Vagrant](https://www.vagrantup.com/) project leverages [Virtual Box](https://www.virtualbox.org/) and [published guest boxes](https://vagrantcloud.com/boxes/search) to automate the provisioning of virtual machines on your local workstation.  You can use the following `Vagrantfile` to stand up an environment for testing the Docker container.

```
# -*- mode: ruby -*-
# vi: set ft=ruby :

$script = <<SCRIPT
add-apt-repository ppa:webupd8team/java
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9
echo deb https://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list
apt-get update

JAVA_VER=8
echo oracle-java${JAVA_VER}-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
apt-get install -y --force-yes --no-install-recommends oracle-java${JAVA_VER}-installer oracle-java${JAVA_VER}-set-default 2>/dev/null

apt-get install -y git lxc-docker

mkdir git
pushd git
git clone https://github.com/Netflix/edda.git
git clone https://github.com/Netflix-Skunkworks/zerotodocker.git
popd
chown -R vagrant:vagrant git
SCRIPT

Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"
  config.vm.provider "virtualbox" do |vb|
    vb.name = "nflxoss_trusty64"
    vb.memory = 4096
    vb.cpus = 2
  end
  config.vm.provision "shell", inline: $script
end
```

To use this `Vagrantfile`:

```
vagrant up
vagrant ssh
sudo docker ...
```

## Testing with Boot2Docker

The [Boot2Docker](http://boot2docker.io/) project leverages [Virtual Box](https://www.virtualbox.org/) and [Tiny Core Linux](http://distro.ibiblio.org/tinycorelinux/) to provide a thin execution environment for Docker containers.  To allow access to the Edda container in the VM from your terminal, you need to configure a [port forwarding rule](https://github.com/docker/docker/issues/4007) on the Virtual Box guest.

```
boot2docker init
VBoxManage modifyvm "boot2docker-vm" --natpf1 "tcp-port7101,tcp,,7101,,7101"
boot2docker up
$(boot2docker shellinit)

docker run \
  --name atlas \
  -p 7101:7101 \
  netflixoss/atlas:1.4.1

curl -s http://localhost:7101/api/v1/tags
```