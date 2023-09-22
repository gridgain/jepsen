#!/bin/bash

export CONTAINERS=2
export NETPREF=192.168.122
export KEYFILE=/home/$USER/.ssh/id_rsa.pub

if [[ ! $USER ]]; then
   echo "Specify USER variable"
   exit 1
fi

if [ `whoami` != 'root' ]; then
    echo "Run as root"
    exit 2
fi

function setup {
  set -ex
	echo "01. Create"
	for (( i=1; i<=$CONTAINERS; i++ )) do lxc-create -n n$i -t debian -- --release buster; done

	echo "02. Config network"
	for (( i=1; i<=$CONTAINERS; i++ )) do

		sed -i -E 's/(.+lxc\.net.+)/#\1/' /var/lib/lxc/n${i}/config

		cat >>/var/lib/lxc/n${i}/config <<EOF

# Network config
lxc.net.0.type = veth
lxc.net.0.flags = up
lxc.net.0.link = virbr0
lxc.net.0.hwaddr = 00:1E:62:AA:AA:$(printf "%02x" $i)
EOF
	done

	echo "03. Network bindings"
	for (( i=1; i<=$CONTAINERS; i++ )) do
	  virsh net-update --current default add-last ip-dhcp-host "<host mac=\"00:1E:62:AA:AA:$(printf "%02x" $i)\" name=\"n${i}\" ip=\"$NETPREF.1$(printf "%02d" $i)\"/>"
	done

	echo "04. Start network"
	virsh net-autostart default;
	virsh net-start default

	echo "05. Add hostnames"
	#echo -e "nameserver ${NETPREF}.1\n$(cat /etc/resolv.conf)" > /etc/resolv.conf

	if ! grep -q "Jepsen hosts" /etc/hosts; then
		echo "# Jepsen hosts" >> /etc/hosts

		for (( i=1; i<=$CONTAINERS; i++ )) do
			echo -e "${NETPREF}.${i} n${i}" >> /etc/hosts
		done
  fi

	echo "06. Setup dhcp client dns"
	echo -e "prepend domain-name-servers ${NETPREF}.1;" >>/etc/dhcp/dhclient.conf
	service networking restart

	echo "07. Copy keys to the lxc containers"
	for (( i=1; i<=$CONTAINERS; i++ )) do
	  mkdir -p /var/lib/lxc/n${i}/rootfs/root/.ssh
	  chmod 700 /var/lib/lxc/n${i}/rootfs/root/.ssh/
	  cp $KEYFILE /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys
	  chmod 644 /var/lib/lxc/n${i}/rootfs/root/.ssh/authorized_keys
	done

	start
	echo "Waiting containers to start"
	sleep 15

	echo "08. Setup SSHD in lxc"
	for (( i=1; i<=$CONTAINERS; i++ )) do
	  sudo lxc-attach -n n${i} -- apt-get update -y
	  sudo lxc-attach -n n${i} -- apt-get install -y openssh-server sudo wget unzip openjdk-11-jdk-headless
	  sudo lxc-attach -n n${i} -- bash -c 'echo -e "root\nroot\n" | passwd root';
	  sudo lxc-attach -n n${i} -- sed -i 's,^#\?PermitRootLogin .*,PermitRootLogin yes,g' /etc/ssh/sshd_config;
	  sudo lxc-attach -n n${i} -- systemctl restart sshd;
	done

	for (( i=1; i<=$CONTAINERS; i++ )) do
	  ssh-keyscan -t rsa n${i} >> ~/.ssh/known_hosts
	done
}

function start {
	echo "09. Start lxc containers"
	set -xe

	for (( i=1; i<=$CONTAINERS; i++ )) do
	  sudo lxc-start -d -n n$i
	done

}

function stop {
    echo "01 Stop LXC"

	for (( i=1; i<=$CONTAINERS; i++ )) do
	  lxc-stop -n n$i
	done
}

function cleanup {
	echo "Removing LXC containers"

	for (( i=1; i<=$CONTAINERS; i++ )) do
		lxc-destroy -n n$i -s
	done

	echo "LXC cleaned"
}

#set -x trace
echo $1

case $1 in

	setup)
    	cleanup
		setup
		;;
	start)
		start
		;;
	stop)
		stop
		;;
	cleanup)
		cleanup
		;;
	*)
		echo -e "Use $0 [setup|start|stop|cleanup]"
		exit 3
esac
