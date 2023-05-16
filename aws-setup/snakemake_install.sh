#!/usr/bin/bash

# download micromamba
curl micro.mamba.pm/install.sh | bash

echo 'export PATH=$PATH:/home/ubuntu/mamba/bin/micromamba' >> ~/.bashrc
source ~/.bashrc

# download micromamba, python, snakemake
micromamba create -y -c conda-forge -c bioconda -n snakemake snakemake-minimal python=3.11.3

# activate snakemake
micromamba activate snakemake