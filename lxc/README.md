# LXC containerst for Jepsen

This bash script attempts to simplify the setup required by Jepsen.
It is intended to be used by a CI tool or anyone with LXC who wants to try Jepsen themselves.

It contains all the jepsen requirements to run Ignite/Ignite3. It uses LXC to spin up the 10 containers used
by Jepsen. 

## Quickstart

Check /lxc/deploy.sh script. If necessary - change:

* CONTAINERS - number of container you need (try to start one to check)
* NETPREF - LXC network prefix (for example: network 192.168.0.0/24 should have 192.168.0)

Assuming you have LXC installed, run (requires root privileges):

```
export USER=<PLACE_YOUR_USERNAME_HERE>
./deploy.sh setup
```

which will set up 10 nodes and set IPs.
The other commands is:

* start - start containters
* stop - stop containter
* cleanup - destroy containers

Your DB nodes are `n1`, `n2`, `n3`, `n4`, and `n5`. You can open as many shells
as you like using `lxc-attach`. If your test includes a web server (try `lein
run serve` on the control node, in your test directory), you can open it
locally by running using `bin/web`. This can be a handy way to browse test
results.

## Prepequisites

Initially need to config LXD on host machine. Run:
 ```bash
 sudo lxd init
 ```
 Init config may look like:

```text
 config:
   images.auto_update_interval: "0"
 networks: []
 storage_pools:
 - config:
     size: 30GiB
   description: ""
   name: default
   driver: zfs
 profiles:
 - config: {}
   description: ""
   devices:
     root:
       path: /
       pool: default
       type: disk
   name: default
 projects: []
 cluster: null
```

# Possible errors:

### 1) If you get keyring error like: The specified keyring /var/cache/lxc/debian/archive-key.gpg may be incorrect or out of date

Just download the new keys and import it with:

```shell
wget "https://ftp-master.debian.org/keys/release-10.asc"
sudo gpg --no-default-keyring --keyring /var/cache/lxc/debian/archive-key.gpg --import release-10.asc
```

### 2) If you get an error tryint to run virsh commands like:
error: failed to connect to the hypervisor
error: Failed to connect socket to '/var/run/libvirt/libvirt-sock': No such file or directory

 Install kvm with:

```shell
 sudo apt install qemu qemu-kvm libvirt-clients libvirt-daemon-system virtinst bridge-utils
 sudo systemctl enable libvirtd
 sudo systemctl start libvirtd
```

