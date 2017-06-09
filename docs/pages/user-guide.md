# DCOS-COMMONS User Guide

The following are instructions for creating a new Mesos framework / DCOS package using dcos-commons.

## Creating a New Framework

These steps will help you get a template framework built, installed, and running on your DCOS cluster. Once you have your framework running, you can customize it as necessary.

1. Clone [dcos-commons](https://github.com/mesosphere/dcos-commons/).
2. Read the [developer guide](https://mesosphere.github.io/dcos-commons/developer-guide.html).
3. Run `./new-framework.sh frameworks/<framework-name>`.
4. Open up and read through `frameworks/<framework-name>/src/main/dist/svc.yml` and `frameworks/<framework-name>/universe/marathon.json.mustache`. These files will be where you need to do most of your work. Reference the [YAML guide](https://mesosphere.github.io/dcos-commons/yaml-reference.html) to understand `svc.yml` and the [Marathon docs](https://mesosphere.github.io/marathon/docs/) to understand `marathon.json.mustache`.
5. Run `cd frameworks/<framework-name>` before building your package.
6. Make sure you have the DCOS cli installed and pointing to your cluster.
7. To build, run `./build.sh <aws | local>`. If using AWS, make sure your credentials are set as environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`). If using local, make sure you have a local DCOS cluster running.
8. After it finishes running, the build script will print instructions for installing/uninstalling your package on DCOS.



## Customizing Your Framework

The next steps diverge here based on the specific needs of your framework. I organized the rest of this section based on common development patterns, so feel free to skim the headings and pick what seems relevant to you. Unless otherwise specified, all instructions assume you are working in `svc.yml`.

### Downloading Packages

Most frameworks need to run some type of server or startup scripts in the `tasks:node:cmd:` field. If you have a hosted copy of the files your framework needs, simply add the URI to `<framework-name>:uris:`. Mesos will download each item in the URI list, extract all packages, and leave the files accessible in the `MESOS_SANDBOX` directory. This path is always accessible from the `MESOS_SANDBOX` environment variable.

### Injecting Configuration Files / Scripts


