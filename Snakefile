import os
import sys
configfile: "config.yaml"


def get_samples():
    with open("../checkout.txt") as handle:
        crams = handle.readlines()
    return [cram.split('.cram')[0].split('/')[-1] for cram in crams]

rule_all = [
    
    expand("str-results/{sample}", sample=get_samples())
]

rule all:
    input: rule_all

rule init:
    shell:
        """
        mkdir -p bin
        mdir -p str-results
        """

rule call_strling:
    input: 
        ref = config["REFERENCE_FASTA"],
        cram = "../crams/{sample}.cram"
    output: directory("str-results/{sample}")
    singularity: "docker://brwnj/strling:latest"
    resources:
        mem_mb = 4000
    threads: 1
    shell:
        """
        mkdir -p str-bins/
        strling extract -f {input.ref} {input.cram} {wildcards.sample}.bin
        mkdir -p str-results/
        strling call --output-prefix {output} -f {input.ref} {input.cram} {wildcards.sample}.bin
        """