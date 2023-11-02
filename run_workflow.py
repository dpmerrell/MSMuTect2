
import pandas as pd
import dalmatian
import prefect
import wolf
import yaml
import sys


class msmutect(wolf.Task):
        name = 'msmutect'
        inputs= {"bam", "bai", "loci", "sample_type", "output_prefix"}
        outputs = {'hist': '*.hist.mot.all'}
        script = [
            """
            lociId=$(echo ${loci} | awk '{ gsub(/.*split_/, ""); print }')
            combp=${output_prefix}.${sample_type}.${lociId}
            python3 /app/msmutect.py -I ${bam} -l ${loci} -O ${combp}.hist
            bash /app/hist2py.sh ${combp}.hist
            python /app/get_all.py  ${combp}.hist.mot /app/P_A.csv \
            > ${combp}.hist.mot.all
            """
        ]
        docker = "gcr.io/broad-getzlab-workflows/msmutect2_wolf:v2"
        resources={"mem": '45G', 'cpus-per-task': '3'}
        use_scratch_disk = True


class lociSplit(wolf.Task):
        name = 'split'
        inputs= {"loci"}
        outputs = {'loci_split':'split*'}
        script = [
            """
            split -l 400000 ${loci} split_
            """
        ]
        docker = "gcr.io/broad-getzlab-workflows/base_image:v0.0.6"
        resources={"mem": '20G', 'cpus-per-task': '2'}
        use_scratch_disk = True


class combineHist(wolf.Task):
          name = "gather"
          inputs = {"histarray","prefix"}
          script = "sort ${histarray} | cat | tr '\n' ' '| xargs cat > ${prefix}.hist.mot.all"
          outputs = {"histall" : "*.hist.mot.all"}
          docker = "gcr.io/broad-getzlab-workflows/base_image:v0.0.6"
          resources={"mem": '20G', 'cpus-per-task': '2'}


def localize_and_msmutect(bam, bai, sample_name, sample_type,
                          sync=False, terra_workspace=None,
                          locus_file="gs://getzlab-workflows-reference_files-oa/hg38/msmutect/hg38_1_to_15_loci.phobos"):

    localized = wolf.LocalizeToDisk(files={"bam": bam,
                                           "bai": bai,
                                           "loci_file": locus_file
                                           }
                                    )

    splitLoci = lociSplit(inputs = dict(loci = localized["loci_file"]))
    msOut = msmutect(inputs=dict(bam=localized["bam"],
                                 bai=localized["bai"],
                                 loci=splitLoci["loci_split"],
                                 sample_type=source,
                                 output_prefix=sample)
                               )

    comb_msOut = combineHist(inputs=dict(histarray=[msOut["hist"]],
                                         prefix=f"{sample_name}.{sample_type}")
                            )

    if sync:
        wolf.SyncToWorkspace(nameworkspace=terra_workspace,
                             entity_type="sample",
                             entity_name=sample_name,
                             attr_map={"msmutect2_histall": comb_msOut["histall"]}
                            )


if __name__=="__main__":

    config = sys.argv[1]

    with open(config, "r") as f:
        config = yaml.safe_load(f)

    terra_workspace = config["terra_workspace"]
    locus_file = config["locus_file"]

    wic = wolf.fc.WorkspaceInputConnector(terra_workspace)
    samples = wic.get_samples()

    samples_df = ???

    with wolf.Workflow(workflow = localize_and_msmutect, conf={"clust_frac": 0.9}) as w:
        for row in samples_df.iterrows():
            w.run(run_name=f"{sample_name}_msmutect2",
                  bam=,
                  bai=,
                  sample_name=,
                  sample_type=,
                  sync=True, terra_workspace=terra_workspace,
                  locus_file=locus_file
                  )

