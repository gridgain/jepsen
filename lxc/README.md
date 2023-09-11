# LXC containerst for Jepsen

This bash script attempts to simplify the setup required by Jepsen.
It is intended to be used by a CI tool or anyone with LXC who wants to try Jepsen themselves.

It contains all the jepsen requirements to run Ignite/Ignite3. It uses LXC to spin up the 10 containers used
by Jepsen. 

## Quickstart

Assuming you have LXC, run (requires root privileges):

```
./Deploy.sh setup
```

which will setup 10 nodes and set IPs.
The other commands is:
* start - start containters
* stop - stop containter
* cleanup - destroy containers

Your DB nodes are `n1`, `n2`, `n3`, `n4`, and `n5`. You can open as many shells
as you like using `lxc-attach`. If your test includes a web server (try `lein
run serve` on the control node, in your test directory), you can open it
locally by running using `bin/web`. This can be a handy way to browse test
results.

