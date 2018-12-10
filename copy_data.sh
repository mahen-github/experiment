#/bin/bash

scp -r -i  ~/.ssh/edhops /home/mqp29/repos/work/myws/repos/workspace_bkp/experiment/src/test/resources/data_demo edhops@apps-data-dev-edge.bdcdev.cas.org:/tmp

ssh -i ~/.ssh/edhops edhops@apps-data-dev-edge.bdcdev.cas.org hadoop fs -put /tmp/data_demo /tmp/data_demo
