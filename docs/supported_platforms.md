# Supported platforms

## AWS

TODO document this platform

### Deployment types

#### IPI

#### UPI

## vSphere

TODO document this platform

### Deployment types

#### UPI

## Azure

File `osServicePrincipal.json` in `~/.azure/` directory with proper credentials
is required. The file looks like this:

```json
{
  "subscriptionId": "...",
  "clientId": "...",
  "clientSecret": "...",
  "tenantId": "..."
}
```

To deploy ocs-ci cluster on Azure platform using one of OCS QE subscriptions,
you need to specify one deployment config file from `conf/deployment/azure/`
directory (which one to use depends on the type of deployment one needs to
use) and also one config file with azure `base_domain` value (either
`conf/ocsci/azure_qe_rh_ocs_com_domain.yaml` or
`conf/ocsci/azure2_qe_rh_ocs_com_domain.yaml` or similar newly created with
proper `base_domain` configuration), because each azure subscription has it's
own domain name.

### Deployment types

#### UPI

#### IPI

##### STS Enabled

In order to enable STS (short-lived token auth) for a cluster deployed to Azure,
you will need to use a config like `conf/deployment/azure/ipi_3az_rhcos_sts_3m_3w.yaml`.

In addition to the standard Azure IPI config items, the following are important here:

```yaml
---
DEPLOYMENT:
  sts_enabled: true
  subscription_plan_approval: "Manual"
ENV_DATA:
  azure_base_domain_resource_group_name: 'odfqe'
```

`sts_enabled: true` is also important for teardown in order to clean up resources in Azure.

## IBM Cloud

This platform supports deployment of OCP cluster + OCS cluster on top of it.
For IPI deployments, please see the IPI section below.

### Requirements

* Have locally installed ibmcloud CLI, follow [official documentation](https://cloud.ibm.com/docs/cli).
* Install required plugins for IBM Cloud CLI via command: `ibmcloud plugin install PLUGIN_NAME -f -r 'IBM Cloud'`
  * infrastructure-service
  * kubernetes-service
* Provide credential config file + credential config file described bellow to
`run-ci` command via `--ocsci-conf` parameters.

### Supported providers

* `vpc-gen2` - This is first implemented supported provider for IBM cloud.
* `classic` - Not supported yet.
* `satellite` - Not supported yet.

### Configuration files

* `conf/deployment/ibmcloud/ibm_cloud_vpc_cluster.yaml` - this file can be used for deployment
  on IBM Cloud - this config is for VPC gen 2 provider.

* `conf/deployment/ibmcloud/ibm_cloud_vpc_cluster_without_cos.yaml` - this file can be used for deployment
  on IBM Cloud - this config is for VPC gen 2 provider without COS secret created.

You will need to create also credential file with secret data which should
never be shared publicly in this repository.
All needed values are mentioned as placeholders and commented out in
`conf/deployment/ibmcloud/ibm_cloud_vpc_cluster.yaml`

But an example is pasted also here:

```yaml
# This is the basic config for IBM cloud usage
---
ENV_DATA:
  vpc_id: VPC ID PLACEHOLDER
  subnet_id: SUBNET ID PLACEHOLDER
  # Subnet id from dict below will be used if user defines worker_availability_zones
  # This is for multi zone deployment support. So If you have worker_availability_zones and
  # subnet_ids_per_zone defined in credential conf it will be used instead of subnet_id.
  subnet_ids_per_zone:
    "eu-de-1": "PLACEHOLDER_SUBNET_ID"
    "eu-de-2": "PLACEHOLDER_SUBNET_ID"
    "eu-de-3": "PLACEHOLDER_SUBNET_ID"
  cos_instance: COS INSTANCE PLACEHOLDER
AUTH:
  ibmcloud:
    api_key: IBM CLOUD API KEY PLACEHOLDER
    account_id: ACCOUNT ID PLACEHOLDER
    ibm_cos_access_key_id: KEY PLACEHOLDER
    ibm_cos_secret_access_key: SECRET PLACEHOLDER
DEPLOYMENT:
  ocs_secret_dockerconfigjson: BASE64 OF QUAY SECRET PLACEHOLDER
```

### How to get API key

API key creation is - one time action and is required for run deployment and
other operation with ibmcloud CLI as we are using login with this API key.

Follow this documentation [access API Key](https://cloud.ibm.com/docs/openshift?topic=openshift-access_cluster#access_api_key).

Command:
`ibmcloud login -u user@redhat.com -p PASSWORD_PLACEHOLDER -c ACCOUNT_ID_PLACEHOLDER -r us-east`

You should get this info as output where you can get the key:

```
Please preserve the API key! It cannot be retrieved after it's created.

ID            ApiKey-ID
Name          my-api-key
Description
Created At    2020-10-19T15:05+0000
API Key       OUR_KEY
Locked        false
```

### How to get COS keys

Please follow official
[documentation](https://cloud.ibm.com/docs/cloud-object-storage?topic=cloud-object-storage-uhc-hmac-credentials-main).

### Deployment types

#### IPI

IPI deployments to IBM Cloud are supported for ODF versions >= 4.10.

##### Requirements

1. OCP installer version >= 4.10
2. ibmcloud ipi config file (e.g. `conf/deployment/ibmcloud/ipi_3az_rhcos_3m_3w.yaml`)
3. ibmcloud account data. This data should appear as follows in the ocsci config:

```
#AUTH:
#  ibmcloud:
#    account_id: ACCOUNT ID PLACEHOLDER
#    api_key: IBM CLOUD API KEY PLACEHOLDER
```

#### Managed

This deployment type is managed - there is no openshift-installer used for
installing the cluster, but we are using ibmcloud CLI to create the cluster.

As we rely on matadata.json file to store cluster name and few other details
like ID we are creating this json file from data which we get from command:
`ibmcloud ks cluster get --cluster CLUSTER_NAME -json` and updating with values
we usually read from this file like `clusterName` and `clusterID`.

The kubeconfig is generated by command:
`ibmcloud ks cluster config --cluster cluster-name --admin --output yaml`

And stored in the same structure like openshift-installer does in cluster dir.

There are added just worker nodes as par to the deployment as master nodes are
not directly accessible in this kind of deployment.

### Known issues

#### Global pull secret

There is no way how to update global pull secret cause of missing
machineConfig. So as workaround to be able install internal build from private
quay repository we need to create secret and add it to two namespaces:

* openshift-marketplace - in order to be able to create catalogSource
* openshift-storage - in order to be able install OCS operator

We need to then assign this secret to all service accounts in mentioned
namespaces.

> Note:
> In openshift-storage namespace there are not all service accounts created
> before deployment. As workaround we need to run deployment - then we see that
> pods will not start cause of image pull error, but service account will be
> created. Then we need to again assign secret to all service accounts and
> remove all pods in the openshift-storage namespace. Then pods will be created
> again and successfully pull the images.

#### Must-gather

There is also problem with secret in must-gather. As must gather is always
created in temporary created namespace with random name we cannot use the same
mechanism mentioned above. As a workaround we are able to use live must-gather
after the GA of specific version. Or we have to use upstream image. By default
we are using upstream image for IBM Cloud.

Config files:

* `/conf/ocsci/live-must-gather.yaml` - for usage of live GAed version of mus
  gather image
* `conf/ocsci/upstream-must-gather.yaml` - for usage of upstream image, this is
  also now part of `conf/deployment/ibmcloud/ibm_cloud_vpc_cluster.yaml`

#### Deployment of OCP takes long time

It happened a lot that OCP cluster didn't get to normal state in several hours
and was in warning status cause of some ingress issue. For now, the timeout is
increased to 10 hours as a workaround.

#### Missing snapshotter CRDs

Some missing CRDs for snapshooter are missing in IBM Cloud.
As a workaround we need to create those CRDs depends on OCP version. (this is
covered in automation)
OCP 4.6 https://github.com/openshift/csi-external-snapshotter/tree/release-4.6/client/config/crd
OCP 4.5 https://github.com/openshift/csi-external-snapshotter/tree/release-4.5/config/crd

## Openshift Dedicated

This platform supports deployment of OCP cluster + OCS cluster on top of it.

### Requirements

* Provide config file + credential config file described bellow to
`run-ci` command via `--ocsci-conf` parameters.

### Configuration files

* `conf/ocsci/openshift_dedicated.yaml` - this file can be used for deployment
    on Openshift Dedicated.

You will need to create also credential file with secret data which should
never be shared publicly in this repository.
All needed values are mentioned as placeholders and commented out in
`conf/ocsci/openshift_dedicated.yaml` and also mentioned below.

```yaml
# This is the basic config for Openshift Dedicated usage
---
AUTH:
 openshiftdedicated:
   token: OCM TOKEN KEY PLACEHOLDER
```

### Deployment types

#### Managed

This deployment type is managed - there is no openshift-installer used for
installing the cluster, but we are using osde2e CLI to create the cluster.

The kubeconfig is generated by command:
`ocm get /api/clusters_mgmt/v1/clusters/{cluster_id}/credentials`

And stored in the same structure like openshift-installer does in cluster dir.

## ROSA

This platform supports deployment of OCP cluster + OCS cluster on top of it.

### Requirements

* Provide config file + credential config file described bellow to
`run-ci` command via `--ocsci-conf` parameters.

### Configuration files

* `conf/ocsci/rosa.yaml` - this file can be used for deployment on ROSA.

You will need to create also credential file with secret data which should
never be shared publicly in this repository.
All needed values are mentioned as placeholders and commented out in
`conf/ocsci/rosa.yaml` and also mentioned below.

```yaml
# This is the basic config for OCM, user needs to login to OCM before ROSA can
# be used
---
AUTH:
 openshiftdedicated:
   token: OCM TOKEN KEY PLACEHOLDER
```

### Deployment types

#### Managed

This deployment type is managed - there is no openshift-installer used for
installing the cluster, but we are using osde2e CLI to create the cluster.

The kubeconfig is generated by command:
`ocm get /api/clusters_mgmt/v1/clusters/{cluster_id}/credentials`

And stored in the same structure like openshift-installer does in cluster dir.
