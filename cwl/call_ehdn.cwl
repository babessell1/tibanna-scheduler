---
cwlVersion: v1.0
class: CommandLineTool
baseCommand:
  - run_ehdn.sh
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
outputs:
  - id: "#out"
    type: File
    outputBinding:
      glob: "out/*.tar"
hints:
  - dockerPull: babessell/ehdn-aws:main
    class: DockerRequirement