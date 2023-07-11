---
cwlVersion: v1.0
class: CommandLineTool
baseCommand:
  - run_melt.sh
inputs:
  - id: "#crams"
    type: File[]
    inputBinding:
      position: 1
  - id: "#fasta"
    type:
      - File
    inputBinding:
      position: 3
  - id: "#fastaidx"
    type:
      - File
    inputBinding:
      position: 4
  - id: "#melt"
    type:
      - File
    inputBinding:
      position: 5

outputs:
  - id: "#out"
    type: File
    outputBinding:
      glob: "out/*.tar"
hints:
  - dockerPull: sop51/melt-aws:main
    class: DockerRequirement
