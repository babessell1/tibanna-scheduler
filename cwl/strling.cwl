#!/usr/bin/env cwl-runner

class: CommandLineTool

cwlVersion: v1.0

baseCommand: [run_strling.sh]


requirements:
  - class: DockerRequirement
    dockerImageId: babessell/strling-unofficial:main

inputs:
    crams_:
        type: File[]
        inputBinding:
            position: 1

    fasta_:
        type: File
        inputBinding:
            position: 3
        
    cramsidx_:
        type: File[]
        inputBinding:
            position: 4
    fastaidx_:
        type: File
        inputBinding:
            position: 6

outputs:
    bin:
        type: File[]
        outputBinding:
            glob: "*.bin"
    bounds:
        type: File[]
        outputBinding:
            glob: "*-bounds.txt"
    genotype:
        type: File[]
        outputBinding:
            glob: "*-genotype.txt"
    unplaced:
        type: File[]
        outputBinding:
            glob: "*-unplaced.txt"
