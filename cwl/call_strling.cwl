---
cwlVersion: v1.0
class: CommandLineTool
baseCommand:
  - run_strling.sh
inputs:
  - id: "#crams"
    type:
        type: array
        items: File
    inputBinding:
      position: 1
  - id: "#fasta"
    type:
      - File
    inputBinding:
        position: 3
  - id: "#cramsidx"
    type:
        type: array
        items: File
    inputBinding:
        position: 5
  - id: "#fastaidx"
    type:
      - File
    inputBinding:
        position: 6

outputs:
  - id: "#call"
    type:
      - File
    outputBinding:
      glob: str-results/
  - id: "#log"
    type:
      - File
    outputBinding:
      glob: str-logs/
hints:
  - dockerPull: babessell/strling-unofficial:main
    class: DockerRequirement