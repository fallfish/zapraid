.. _sec-getting-started:

=================
 Getting Started
=================

Previously a **quick start** guide was available. However, it usually left one
asking questions, and thus, did not as the name suggests; get you started
quickly. Thus, a fuller story on getting started using **xNVMe** is provided
here.  Needlessly, this section will have subsections that you can skip and
revisit only in case you find **xNVMe**, or the system you are running on, to
be misbehaving.

If you have read through, and still have questions, then please raise a
issue_, start an asynchronous discussion_, or go to Discord_ for
synchronous interaction.

The task of getting started with **xNVMe** will take you through
:ref:`sec-building` with a companion section on :ref:`sec-building-toolchain`
prerequisites, followed by a section describing runtime requirements in
:ref:`sec-gs-system-config`, ending with an example of
:ref:`sec-building-example`.

.. _sec-building:

Building xNVMe
==============

**xNVMe** builds and runs on Linux, FreeBSD and Windows. First, retrieve the
**xNVMe** repository from  `GitHUB <https://github.com/OpenMPDK/xNVMe>`_:

.. include:: clone.rst

Before you invoke the compilation, then setup your toolchain, that is, ensure
that you you have the compiler, build-tools, and auxilary packages needed. The
:ref:`sec-building-toolchain` section describes what to install, and how, on
rich selection of Linux distributions, FreeBSD and Windows.

With that out of the way, then go ahead:

.. include:: build_meson.rst

.. note:: Details on the build-errors can be seen by inspecting
   ``builddir/meson-logs/meson-log.txt``.

.. note:: In case you ran the meson-commands before installing, then you can
   probably need to remove your ``builddir`` before re-running build commands.

In case you want to customize the build, e.g. install into a differnt location
etc. then this is all handled by `meson built-in options
<https://mesonbuild.com/Builtin-options.html>`_, in addition to those, then you
can inspect ``meson_options.txt`` which contains build-options specific to
**xNVMe**. For examples on customizing the build then have a look in the 
look at the following :ref:`sec-building-config`.

Otherwise, with a successfully built and installed **xNVMe**, then jump to
:ref:`sec-gs-system-config` and :ref:`sec-building-example`.

.. _sec-building-toolchain:

Toolchain
=========

The toolchain (compiler, archiver, and linker) used for building **xNVMe**
must support **C11**, **pthreads** and on the system the following tools must
be available:

* Python (>=3.7)
* meson (>=0.61)
* ninja (>=)
* make (gmake)
* gcc/mingw/clang

Along with libraries:

* glibc (>= 2.28, for **io_uring/liburing**)
* libaio-dev (>=0.3, For **xNVMe** and **SPDK**)
* libnuma-dev (>=2, For **SPDK**)
* libssl-dev (>=1.1, For **SPDK**)
* uuid-dev (>=2.3, For **SPDK**)

The preferred toolchain is **gcc** and the following sections describe how to
install it and required libraries on FreeBSD, a set of popular Linux
Distributions and Windows.

If you which to use a different toolchain then see the
:ref:`sec-building-custom-toolchain`, on how to instrument the build-system
using a compiler other than **gcc**.

Alpine Linux
------------

From the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/alpine-latest.sh
   :language: bash
   :lines: 8-

This will install the packages listed below via the sytems package manager.

.. literalinclude:: ../../toolbox/pkgs/alpine-latest.txt
   :language: bash

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/alpine-latest-build.sh
   :language: bash
   :lines: 2-

.. note:: There are issues with SPDK/DPDK due to incompatibilities with the
   standard library provided by ``musl libc``. Pull-requests fixing this is
   most welcome, until then, disable support for the SPDK NVMe driver as the
   ``meson setup`` command above.

Arch Linux
----------

Install the following packages via ``pacman``:

.. literalinclude:: ../../toolbox/pkgs/archlinux-latest.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/archlinux-latest.sh
   :language: bash
   :lines: 8-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

CentOS 7
--------

Install the following packages via ``yum``:

.. literalinclude:: ../../toolbox/pkgs/centos-centos7.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/centos-centos7.sh
   :language: bash
   :lines: 8-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/centos-centos7-build.sh
   :language: bash
   :lines: 2-

Debian 12 (Bookworm)
--------------------

Install the following packages via ``apt-get`` and ``aptitude``:

.. literalinclude:: ../../toolbox/pkgs/debian-bookworm.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/debian-bookworm.sh
   :language: bash
   :lines: 17-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

Debian 11 (Bullseye)
--------------------

Install the following packages via ``apt-get``, ``aptitude`` and ``pip3``:

.. literalinclude:: ../../toolbox/pkgs/debian-bullseye.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/debian-bullseye.sh
   :language: bash
   :lines: 17-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

Debian 10 (Buster)
------------------

Install the following packages via ``apt-get``, ``aptitude`` and ``pip3``:

.. literalinclude:: ../../toolbox/pkgs/debian-buster.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/debian-buster.sh
   :language: bash
   :lines: 17-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

Debian 9 (Stretch)
------------------

From the root of the **xNVMe** repository, do:

.. literalinclude:: ../../toolbox/pkgs/debian-stretch.sh
   :language: bash
   :lines: 17-

The above will install following packages seen below via the system
package-manager and via ``pip3``:

.. literalinclude:: ../../toolbox/pkgs/debian-stretch.txt
   :language: bash

Then go ahead and configure, build and install using ``meson``:

.. note:: Debian 9 / stretch is EOL, LTS support is available EOL June 2022.
   After this date xNVMe will stop supporting it. Currently, liburing 2.1 is
   not supported, so dropping that via configuration options.

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

Fedora (36)
-----------

From the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/fedora-36.sh
   :language: bash
   :lines: 8-

The above will install the packages seen below via the system package-manager
``dnf``:

.. literalinclude:: ../../toolbox/pkgs/fedora-36.txt
   :language: bash

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-


Fedora (35)
-----------

From the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/fedora-35.sh
   :language: bash
   :lines: 8-

The above will install the packages seen below via the system package-manager
``dnf``:

.. literalinclude:: ../../toolbox/pkgs/fedora-35.txt
   :language: bash

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-


Fedora (34)
-----------

From the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/fedora-34.sh
   :language: bash
   :lines: 8-

The above will install the packages seen below via the system package-manager
``dnf``:

.. literalinclude:: ../../toolbox/pkgs/fedora-34.txt
   :language: bash

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-


Freebsd 13
----------

Ensure that you have kernel source in ``/usr/src``, then install the following
packages via ``pkg``:

.. literalinclude:: ../../toolbox/pkgs/freebsd-13.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/freebsd-13.sh
   :language: bash
   :lines: 8-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

Gentoo
------

Install the following packages using ``emerge``:

.. literalinclude:: ../../toolbox/pkgs/gentoo-latest.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/gentoo-latest.sh
   :language: bash
   :lines: 9-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/gentoo-latest-build.sh
   :language: bash
   :lines: 2-

.. note:: In case you get: ``error adding symbols: DSO missing from command
   line``, during compilation, then add ``-ltinfo -lnurces`` to ``LDFLAGS`` as
   it is done in the commands above.

macOS
--------------------

Install the following packages via ``brew`` and ``pip3``:

.. literalinclude:: ../../toolbox/pkgs/macos-11.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/macos-11.sh
   :language: bash
   :lines: 3-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

openSUSE Tumbleweed
-------------------

Install the following packages via ``zypper``:

.. literalinclude:: ../../toolbox/pkgs/opensuse-tumbleweed-latest.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/opensuse-tumbleweed-latest.sh
   :language: bash
   :lines: 8-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

openSUSE Leap (15.4)
--------------------

Install the following packages via ``zypper``:

.. literalinclude:: ../../toolbox/pkgs/opensuse-leap-15.4.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/opensuse-leap-15.4.sh
   :language: bash
   :lines: 8-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

openSUSE Leap (15.3)
--------------------

Install the following packages via ``zypper``:

.. literalinclude:: ../../toolbox/pkgs/opensuse-leap-15.3.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/opensuse-leap-15.3.sh
   :language: bash
   :lines: 8-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

Ubuntu 21.10 (Impish)
---------------------

Install the following packages via ``apt-get`` and ``pip3``:

.. literalinclude:: ../../toolbox/pkgs/ubuntu-impish.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/ubuntu-impish.sh
   :language: bash
   :lines: 17-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

Ubuntu 20.04 (Focal)
--------------------

Install the following packages via ``apt-get`` and ``pip3``:

.. literalinclude:: ../../toolbox/pkgs/ubuntu-focal.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/ubuntu-focal.sh
   :language: bash
   :lines: 17-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

Ubuntu 18.04 (Bionic)
---------------------

Install the following packages via ``apt-get`` and ``pip3``:

.. literalinclude:: ../../toolbox/pkgs/ubuntu-bionic.txt
   :language: bash

For example, from the root of the **xNVMe** source repository, do:

.. literalinclude:: ../../toolbox/pkgs/ubuntu-bionic.sh
   :language: bash
   :lines: 17-

Then go ahead and configure, build and install using ``meson``:

.. literalinclude:: ../../toolbox/pkgs/default-build.sh
   :language: bash
   :lines: 2-

Windows
-------

You can build **xNVMe** using the MSVC toolchain, however, some of the features
are lost when doing so, specifcally the **xNVMe** fio io-engine. Thus, the
instructions provided here utilizes **msys2**.

The toolchain setup is a bit involved, it **bootstraps** by installing the
Chocolatey package manager by opening an eleveted PowerShell and running::

  Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

Then continues with installing a couple of tools via Chocolatey, then the
actual compiler toolchain via the msys2. A script stays more than a thousand
words, so please consult the ``toolbox`` scripts::

  toolbox/pkgs/windows-2019.bat
  toolbox/pkgs/windows-2022.bat

For details.

To utilize the scripts, then invoke the script in an elevated command-prompt
(``cmd.exe`` as Administrator)::

  cd toolbox\pkgs
  windows-2019.bat

.. note:: in case you see .dll loader-errors, then check that the environment
   variable ``PATH`` contains the various library locations of the the
   toolchain.


.. _sec-gs-system-config:

Backends and System Config
==========================

**xNVMe** relies on certain Operating System Kernel features and infrastructure
that must be available and correctly configured. This subsection goes through
what is uses on Linux and how check whether is it available.

Backends
--------

The purpose of **xNVMe** backends are to provide an instrumental runtime
supporting the **xNVMe** API in a single library with **batteries included**.

That is, it comes with the essential third-party libraries bundled into the
**xNVMe** library. Thus, you get a single C API to program against and a single
library to link with. An similarly for the command-line tools; a single binary
to communicating with devices via the I/O stacks that available on the system.

To inspect the libraries which **xNVMe** is build against and the
supported/enabled backends then invoke:

.. literalinclude:: xnvme_library-info.cmd
   :language: bash

It should produce output similar to:

.. literalinclude:: xnvme_library-info.out
   :language: bash

The ``xnvme_3p`` part of the output informs about the third-party projects
which **xNVMe** was built against, and in the case of libraries, the version it
has bundled.

Although a single API and a single library is provided by **xNVMe**, then
runtime and system configuration dependencies remain. The following subsections
describe how to instrument **xNVMe** to utilize the different kernel interfaces
and user space drivers.

Kernel
------

Linux Kernel version 5.9 or newer is currently preferred as it has all the
features which **xNVMe** utilizes. This section also gives you a brief overview
of the different I/O paths and APIs which the **xNVMe** API unifies access to.

NVMe Driver and IOCTLs
~~~~~~~~~~~~~~~~~~~~~~

The default for **xNVMe** is to communicate with devices via the operating
system NVMe driver IOCTLs, specifically on Linux the following are used:

* ``NVME_IOCTL_ID``
* ``NVME_IOCTL_IO_CMD``
* ``NVME_IOCTL_ADMIN_CMD``
* ``NVME_IOCTL_IO64_CMD``
* ``NVME_IOCTL_ADMIN64_CMD``

In case the ``*64_CMD`` IOCTLs are not available then **xNVMe** falls back to
using the non-64bit equivalents. The 64 vs 32 completion result mostly affect
commands such as Zone Append. You can check that this interface is behaving as
expected by running:

.. literalinclude:: xnvme_info_default.cmd
   :language: bash

Which you yield output equivalent to:

.. literalinclude:: xnvme_info_default.out
   :language: bash
   :lines: 1-10

This tells you that **xNVMe** can communicate with the given device identifier
and it informs you that it utilizes **nvme_ioctl** for synchronous command
execution and it uses **thr** for asynchronous command execution.  Since IOCTLs
are inherently synchronous then **xNVMe** mimics asynchronous behavior over
IOCTLs to support the asynchronous primitives provided by the **xNVMe** API.

Block Layer
~~~~~~~~~~~

In case your device is **not** an NVMe device, then the NVMe IOCTLs won't be
available. **xNVMe** will then try to utilize the Linux Block Layer and treat
a given block device as a NVMe device via shim-layer for NVMe admin commands
such as identify and get-features.

A brief example of checking this:

.. literalinclude:: xnvme_info_block.cmd
   :language: bash

Yielding:

.. literalinclude:: xnvme_info_block.out
   :language: bash

Block Zoned IOCTLs
~~~~~~~~~~~~~~~~~~

Building on the Linux Block model, then the Zoned Block Device model is also
utilized, specifically the following IOCTLs:

* ``BLK_ZONE_REP_CAPACITY``
* ``BLKCLOSEZONE``
* ``BLKFINISHZONE``
* ``BLKOPENZONE``
* ``BLKRESETZONE``
* ``BLKGETNRZONES``
* ``BLKREPORTZONE``

When available, then **xNVMe** can make use of the above IOCTLs. This is
mostly useful when developing/testing using Linux Null Block devices.
And similar for a Zoned NULL Block instance:

.. literalinclude:: xnvme_info_zoned.cmd
   :language: bash

Yielding:

.. literalinclude:: xnvme_info_zoned.out
   :language: bash

Async I/O via ``libaio``
~~~~~~~~~~~~~~~~~~~~~~~~

When AIO is available then the NVMe NVM Commands for **read** and **write** are
sent over the Linux AIO interface. Doing so improves command-throughput at
higher queue-depths when compared to sending the command over via the NVMe
driver ioctl().

One can explicitly tell **xNVMe** to utilize ``libaio`` for async I/O by
encoding it in the device identifier, like so:

.. literalinclude:: xnvme_io_async_read_libaio.cmd
   :language: bash

Yielding the output:

.. literalinclude:: xnvme_io_async_read_libaio.out
   :language: bash
   :lines: 1-12

Async. I/O via ``io_uring``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**xNVMe** utilizes the Linux **io_uring** interface, its support for
feature-probing the **io_uring** interface and the **io_uring** opcodes:

* ``IORING_OP_READ``
* ``IORING_OP_WRITE``

When available, then **xNVMe** can send the NVMe NVM Commands for **read** and
**write** via the Linux **io_uring** interface. Doing so improves
command-throughput at all io-depths when compared to sending the command via
NVMe Driver IOCTLs and libaio. It also leverages the **io_uring** interface to
enabling I/O polling and kernel-side submission polling.

One can explicitly tell **xNVMe** to utilize ``io_uring`` for async I/O by
encoding it in the device identifier, like so:

.. literalinclude:: xnvme_io_async_read_io_uring.cmd
   :language: bash

Yielding the output:

.. literalinclude:: xnvme_io_async_read_io_uring.out
   :language: bash
   :lines: 1-12

.. _sec-gs-system-config-userspace:

User Space
----------

Linux provides the **Userspace I/O** (`uio`_) and **Virtual Function I/O**
`vfio`_ frameworks to write user space I/O drivers. Both interfaces work by
binding a given device to an in-kernel stub-driver. The stub-driver in turn
exposes device-memory and device-interrupts to user space. Thus enabling the
implementation of device drivers entirely in user space.

Although Linux provides a capable NVMe Driver with flexible IOCTLs, then a user
space NVMe driver serves those who seek the lowest possible command per-command
processing overhead or wants full control over NVMe command construction,
including command-payloads.

Fortunately, you do not need to go and write a user space NVMe driver since a
highly efficient, mature and well-maintained driver already exists. Namely, the
NVMe driver provided by the **Storage Platform Development Kit** (_`SPDK`).

Another great fortune is that **xNVMe** bundles the SPDK NVMe Driver with the
**xNVMe** library. So, if you have built and installed **xNVMe** then the
**SPDK** NVMe Driver is readily available to **xNVMe**.

The following subsections goes through a configuration checklist, then shows
how to bind and unbind drivers, and lastly how to utilize non-devfs device
identifiers by enumerating the system and inspecting a device.

.. _sec-gs-system-config-userspace-config:

Config
~~~~~~

What remains is checking your system configuration, enabling IOMMU for use by
the ``vfio-pci`` driver, and possibly falling back to the ``uio_pci_generic``
driver in case ``vfio-pci`` is not working out.  ``vfio`` is preferred as
hardware support for IOMMU allows for isolation between devices.

1) Verify that your CPU supports virtualization / VT-d and that it is enabled
   in your board BIOS.

2) Enable your kernel for an intel CPU then provide the kernel option
   ``intel_iommu=on``.  If you have a non-Intel CPU then consult documentation
   on enabling VT-d / IOMMU for your CPU.

3) Increase limits, open ``/etc/security/limits.conf`` and add:

::

  *    soft memlock unlimited
  *    hard memlock unlimited
  root soft memlock unlimited
  root hard memlock unlimited

Once you have gone through these steps, and rebooted, then this command:

.. literalinclude:: 200_dmesg.cmd
   :language: bash

Should output:

.. literalinclude:: 200_dmesg.out
   :language: bash

And this this command:

.. literalinclude:: 300_find.cmd
   :language: bash

Should have output similar to:

.. literalinclude:: 300_find.out
   :language: bash

Unbinding and binding
~~~~~~~~~~~~~~~~~~~~~

With the system configured then you can use the ``xnvme-driver`` script bind
and unbind devices. The ``xnvme-driver`` script is a merge of the **SPDK**
``setup.sh`` script and its dependencies.

By running the command below **8GB** of hugepages will be configured, the
Kernel NVMe driver unbound, and ``vfio-pci`` bound to the device:

.. literalinclude:: 010_xnvme_driver.cmd
   :language: bash

The command above should produce output similar to:

.. literalinclude:: 010_xnvme_driver.out
   :language: bash

To unbind from ``vfio-pci`` and back to the Kernel NVMe driver, then run:

.. literalinclude:: 500_xnvme_driver_reset.cmd
   :language: bash

Should output similar to:

.. literalinclude:: 500_xnvme_driver_reset.out
   :language: bash

.. _sec-backends-spdk-identifiers:

Device Identifiers
~~~~~~~~~~~~~~~~~~

Since the Kernel NVMe driver is unbound from the device, then the kernel no
longer know that the PCIe device is an NVMe device, thus, it no longer lives in
Linux devfs, that is, no longer available in ``/dev`` as e.g. ``/dev/nvme0n1``.

Instead of the filepath in devfs, then you use PCI ids and xNVMe options.

As always, use the ``xnvme`` cli tool to enumerate devices:

.. literalinclude:: 400_xnvme_enum.cmd
   :language: bash

.. literalinclude:: 400_xnvme_enum.out
   :language: bash

Notice that multiple URIs using the same PCI id but with different **xNVMe**
``?opts=<val>``. This is provided as a means to tell **xNVMe** that you want to
use the NVMe controller at ``0000:03:00.0`` and the namespace identified by
``nsid=1``.

.. literalinclude:: 410_xnvme_info.cmd
   :language: bash

.. literalinclude:: 410_xnvme_info.out
   :language: bash


Similarly, when using the API, then you would use these URIs instead of
filepaths::

  ...
  struct xnvme_dev *dev = xnvme_dev_open("pci:0000:01:00.0?nsid=1");
  ...

.. _sec-building-example:

Building an xNVMe Program
=========================

At this point you should have **xNVMe** built and installed on your system and
have the system correctly configured and you should by now also be familiar
with how to instrument **xNVMe** to utilize different backends and backend
options.

With all that in place, go ahead and compile your own **xNVMe** program.

Example code
------------

This `"hello-world"` example prints out device information of the NVMe
device at ``/dev/nvme0n1``.

To use **xNVMe** include the ``libxnvme.h`` header in your C/C++ source:

.. literalinclude:: hello.c
   :language: c

Compile and link
----------------

A pkg-config is provided with xNVMe, you can use ``pkg-config`` to get the
required linker flags:

.. literalinclude:: pkg_config_00.cmd
   :language: bash

This will output something like the output below, it will vary depending on the
features enabled/disabled.

.. literalinclude:: pkg_config_00.out
   :language: bash

You can pass the arguments above to your compiler, or using pkg-config like so:

.. literalinclude:: hello_00.cmd
   :language: bash

.. note:: You do not need to link with SPDK/DPDK/liburing, as these are bundled
  with **xNVMe**. However, do take note of the linker flags surrounding
  ``-lxnvme``, these are required as SPDK makes use of
  ``__attribute__((constructor))``. Without the linker flags, none of the SPDK
  transports will work, as **ctors** will be "linked-out", and **xNVMe** will
  give you errors such as **device not found**.

Also, **xNVMe** provides three different libraries, two static, and a shared
library as well. Here is what the different libraries are intended for:

* ``libxnvme.a``, this is static library version of **xNVMe** and it comes with
  **batteries included**, that is, all the third-party libraries are bundled
  within the static library ``libxnvme.a``. Thus you only need to link with
  **xNVMe**, as described above, and need not worry about linking with SPDK,
  liburing etc.
* ``libxnvme-static.a``, this is the same as ``libxnvme.a`` except it does
  **not** come with **batteries included**, so you have to manually link with
  SPDK, liburing, etc.
* ``libxnvme-shared.so``, this is the shared library version of **xNVMe**, this
  also does **not** come with **batteries included**, so when linking with, or
  dynamically loading, the shared library version of **xNVMe** then you also
  have to link or load the third-party dependencies.

Using ``libxnvme.a`` is the preferred way to consume **xNVMe** as it comes with
the correct version of the various third-party libraries and provides for
a simpler link-target.

Run!
----

.. literalinclude:: hello_01.cmd
   :language: bash

.. literalinclude:: hello_01.out
   :language: bash

This should conclude the getting started guide of **xNVMe**, go ahead and
explore the :ref:`sec-tools`, :ref:`sec-c-api`, and :ref:`sec-examples`.

Should **xNVMe** or your system still be misbehaving, then take a look in the
:ref:`sec-troubleshooting` section or reach out by raising an issue_, start an
asynchronous discussion_, or go to Discord_ for synchronous interaction.

.. _sec-troubleshooting:

Troubleshooting
===============

User space
----------

In case you are having issues using running with SPDK backend / using then make
sure you following the config section
:ref:`sec-gs-system-config-userspace-config` and if issues persist a solution
might be found in the following subsections.

No devices found
~~~~~~~~~~~~~~~~

When running ``xnvme enum`` then the output-listing is empty, there are no
devices. When running with ``vfio-pci`` then this can occur when your devices
are sharing iommu-group with other devices which are still bound to in-kernel
drivers. This could be NICs, GPUs or other kinds of peripherals.

The division of devices into groups is not something that can be easily
switched, but you try to manually unbind the other devices in the iommu group
from their kernel drivers.

If that is not an option then you can try to re-organize your physical
connectivity of deviecs, e.g. move devices around.

Lastly you can try using ``uio_pci_generic`` instead, this can most easily be
done by disabling iommu by adding the kernel option: ``iommu=off`` to the
kernel command-line and rebooting.

Memory Issues
~~~~~~~~~~~~~

If you see a message similar to the below while unbind devices::

  Current user memlock limit: 16 MB

  This is the maximum amount of memory you will be
  able to use with DPDK and VFIO if run as current user.
  To change this, please adjust limits.conf memlock limit for current user.

  ## WARNING: memlock limit is less than 64MB
  ## DPDK with VFIO may not be able to initialize if run as current user.

Then go you should do as suggested, that is, adjust ``limits.conf``, see for an
example of doing here :ref:`sec-gs-system-config-userspace-config`.

Build Errors
------------

If you are getting errors while attempting to configure and build **xNVMe**
then it is likely due to one of the following:

* You are building in an **offline** environment and only have a shallow
  source-archive or a git-repository without subprojects.

The full source-archive is made available with each release and downloadble
from the `GitHUB Release page <https://github.com/OpenMPDK/xNVMe/releases>`_
release page. It contains the xNVMe source code along with all the third-party
dependencies, namely: SPDK, liburing, libnvme, and fio.

**missing dependencies / toolchain**

* You are missing dependencies, see the :ref:`sec-building-toolchain` for
  installing these on FreeBSD and a handful of different Linux Distributions

The :ref:`sec-building-toolchain` section describes preferred ways of
installing libraries and tools. For example, on Ubuntu 18.04 it is preferred to
install ``meson`` via ``pip3`` since the version in the package registry is too
old for SPDK, thus if installed via the package manager then you will
experience build errors as the xNVMe build system starts building SPDK.

Once you have the full source of xNVMe, third-party library dependencies, and
setup the toolchain then run the following to ensure that the xNVMe repository
is clean from any artifacts left behind by previous build failures::

  make clobber

And then go back to the :ref:`sec-building` and follow the steps there.

Known Build Issues
------------------

If the above did not sort out your build-issues, then you might be facing one
of the following known build-issues. If these do not apply to you, then please
post an issue on GitHUB describing your build environment and output from the
failed build.

When building **xNVMe** on **Alpine Linux** you might encounter some issues due
to musl standard library not being entirely compatible with **GLIBC** /
**BSD**.

The SPDK backend does not build on due to re-definition of ``STAILQ_*``
macros. As a work-around, then disable the SPDK backend::

  meson setup builddir -Dwith-spdk=false

The Linux backend support for ``io_uring`` fails on **Alpine Linux** due to a
missing definition in **musl** leading to this error message::

  include/liburing.h:195:17: error: unknown type name 'loff_t'; did you mean
  'off_t'?

As a work-around, then disable ``io_uring`` support::

  meson setup builddir -Dwith-liburing=false

See more details on changing the default build-configuration of **xNVMe** in
the section :ref:`sec-building-config`.

.. _sec-building-custom:

Customizing the Build
=====================

.. _sec-building-custom-toolchain:

Non-GCC Toolchain
-----------------

To use a compiler other than **gcc**, then:

1) Set the ``CC`` and ``CXX`` environment variable for the ``./configure`` script
2) Pass ``CC``, and ``CXX`` as arguments to ``make``

For example, compiling **xNVMe** on a system where the default compiler is not
**gcc**:

.. code-block:: bash

  CC=gcc CXX=g++ ./configure <YOUR_OPTIONS_HERE>
  make CC=gcc CXX=g++
  make install CC=gcc CXX=g++

Recent versions of **icc**, **clang**, and **pgi** should at be able to satisfy
the **C11** and **pthreads** requirements. However, it will most likely require
a bit of fiddling.

.. note:: The **icc** works well after you bought a license and installed it
  correctly. There is a free option with Intel System Suite 2019.

.. note:: The **pgi** compiler has some issues linking with **SPDK/DPDK** due
  to unstable **ABI** for **RTE** it seems.

The path of least fiddling around is to just install the toolchain and
libraries as described in the :ref:`sec-building-toolchain` section.

.. _sec-building-config:

Custom Configuration
--------------------

See the list of options in ``meson_options.txt``, this file options the
different non-generic options that you can toggle. For traditional
build-configuration such as ``--prefix`` then these are managed like all other
meson-based builds::

  meson setup builddir -Dprefix=/foo/bar

See: https://mesonbuild.com/Builtin-options.html

For details

.. _sec-building-crosscompiling:

Cross-compiling for ARM on x86
------------------------------

This is managed by like any other meson-based build, see:

https://mesonbuild.com/Cross-compilation.html

for details

.. _issue: https://github.com/OpenMPDK/xNVMe/issues

.. _discussion: https://github.com/OpenMPDK/xNVMe/discussions

.. _Discord: https://discord.com/invite/XCbBX9DmKf

.. _vfio: https://www.kernel.org/doc/Documentation/vfio.txt

.. _uio: https://www.kernel.org/doc/html/v4.14/driver-api/uio-howto.html

.. _Chocolatey: https://chocolatey.org/

.. _MinGW: https://www.mingw-w64.org/
