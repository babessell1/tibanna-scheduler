import os
import sys
configfile: "config.yaml"


def get_samples():
    with open("checkout.txt") as handle:
        crams = handle.readlines()
    return [f"checkout/{(cram.split(".cram")[0]).split("/")[-1]}" for cram in crams]

rule_all = [
    expand("bins/{sample}.bin", sample=get_samples())
]

rule all:
    input:

rule index_strling:
    input: config["REFERENCE_FASTA"]
    output: "".join([config["REFERENCE_FASTA"], ".bed"])
    singularity: "docker://brwnj/strling:latest
    shell: 
        """
        strling index {input}
        """ 


rule call_strling:
    input: 
        ref = config["REFERENCE_FASTA"]
        cram = "crams/{sample}.cram"
    output: "bins/{sample}.bin"
    singularity: "docker://brwnj/strling:latest
    shell:
        """
        mkdir -p str-bins/
        strling extract -f {input.ref} {input.cram} {wild.sample}.bin
        mkdir -p str-results/
        strling call --output-prefix str-results/{params.sample} -f {input.ref} {input.cram} {output}
        """