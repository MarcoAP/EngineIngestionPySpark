#!/bin/bash
export ANACONDA=/opt/anaconda2
export PATH=$ANACONDA/bin:$PATH
export PYTHONPATH=/opt/cloudera/parcels/CDH-5.13.2-1.cdh5.13.2.p0.3/lib/spark/python/lib/py4j-0.9-src.zip:/opt/cloudera/parcels/CDH-5.13.2-1.cdh5.13.2.p0.3/lib/spark/python:/opt/anaconda2/lib/python27.zip:/opt/anaconda2/lib/python2.7:/opt/anaconda2/lib/python2.7/plat-linux2:/opt/anaconda2/lib/python2.7/lib-tk:/opt/anaconda2/lib/python2.7/lib-old:/opt/anaconda2/lib/python2.7/lib-dynload:/opt/anaconda2/lib/python2.7/site-packages
export MOTORFI=/projetos/plfinanceiro/bin/
export PYTHONPATH=$PYTHONPATH:$MOTORFI
export PATH=$PATH:$PYTHONPATH
source /bid/common/function/balance_crg_hdp.sh
#kinit -kt /home/sv3plfinanceiro/security/sv3plfinanceiro.keytab sv3plfinanceiro/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA
