#/bin/bash

./copy_data.sh

scp -i  ~/.ssh/edhops /home/mqp29/myRepos/experiment/target/experiment-1.0.1-SNAPSHOT.jar edhops@apps-data-dev-edge.bdcdev.cas.org:/tmp

ssh -i  ~/.ssh/edhops edhops@apps-data-dev-edge.bdcdev.cas.org "spark2-submit --name Mahendran_WC_`date +%F_%T` --class dev.mahendran.templates.PopulationByCountry --conf spark.yarn.submit.waitAppCompletion=false --master yarn --deploy-mode cluster --queue edhops /tmp/experiment-1.0.1-SNAPSHOT.jar"
