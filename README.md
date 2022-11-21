# RAID-6

This repository contains code for CE7490 Project 2: RAID-6 based distributed storage system.

- [RAID-6](#raid-6)
  - [Roadmap](#roadmap)
  - [Installation](#installation)
  - [Quick Start](#quick-start)

## Roadmap

The high level objective of the project is to build a reliable distributed storage system supporting the following basic functionalities:

- [ ] Store and access abstract “data objects” across storage nodes using RAID-6 for fault-tolerance
- [ ] Include mechanisms to determine failure of storage nodes
- [ ] Carry out rebuild of lost redundancy at a replacement storage node.

On top of the minimal implementation, further features as showed below are also encouraged to consider.

- [ ] In the objective “1” above, it is deliberately vague as to what data objects mean. You can consider them as something like x-MB data chunks. However, actual files may be much smaller, or larger than the quantum of data which is treated as an object in the system. In a practical system, it is necessary to accommodate real files of arbitrary size, taking into account issues like RAID mapping, etc.
- [ ] Instead of using folders to emulate nodes, extend the implementation to work across multiple (virtual) machines to realize a peer-to-peer RAIN.
- [ ] Support mutable files, taking into account update of the content, and consistency issues.
- [ ] Support larger set of configurations (than just 6+2, using a more full-fledged implementation).
- [ ] Optimize the computation operations.

## Installation

<details>
  <summary>Click here to see how to install docker & docker-compose</summary>

1. Install Docker, simply run

   ```shell
   curl <https://get.docker.com> | sh
   ```

2. Install docker-compose

   ```shell
   sudo curl -L "[https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)](https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname%20-s)-$(uname%20-m))" -o /usr/local/bin/docker-compose

   sudo chmod +x /usr/local/bin/docker-compose
   ```

</details>

<br/>

We dockerized the environment into an image. You can build it yourself with `docker/Dockerfile.ce7490_proj2` .

Or you can use the off the shelf image with `docker-compose.yml`.

Create the container with

```shell
docker-compose up -d
```

Attach to the container with

```shell
docker attach {container name}
```

Detach from the container without stopping it, press `ctrl+p` and `ctrl+q`.

## Quick Start

Start the test pipeline

```shell
python test_raid6.py
```
