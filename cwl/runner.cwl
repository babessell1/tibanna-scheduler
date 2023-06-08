#!/usr/bin/env cwl-runner

class: CommandLineTool
cwlVersion: v1.0

inputs:
  crams:
    type: File[]
  fasta:
    type: File
  cramsidx
    type: File[]
  fastaidx
    type: File

outputs:
  bin:
    type: File[]
    outputSource: strling/bin
  bounds:
    type: File[]
    outputSource: strling/bounds
  genotype:
    type: File[]
    outputSource: strling/genotype
  unplaced:
    type: File[]
    outputSource: strling/unplaced

steps:
  strling:
    run: strling.cwl
    in:
      crams_:
        source: crams
      fasta_:
        source: fasta
      cramsidx_:
        source: cramsidx
      fastaidx_:
        source: fastaidx
    out: [bin, bounds, genotype, unplaced]
