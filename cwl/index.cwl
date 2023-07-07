---
cwlVersion: v1.0
class: CommandLineTool
baseCommand:
  - run_index.sh
inputs:
  - id: "#crams"
    type: File[]
    inputBinding:
      position: 1
outputs:
  - id: "#out"
    type: File
    outputBinding:
      glob: "cramsidx/*.tar"
hints:
  - dockerPull: babessell/tib-indexer:main
    class: DockerRequirement