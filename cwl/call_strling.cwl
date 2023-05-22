---
cwlVersion: v1.0
baseCommand:
  - run_strling.sh
inputs:
  - id: "#cramfile"
    type:
      - File
    inputBinding:
      position: 1
  - id: "#fastafile"
    type:
      - File
    inputBinding:
        position: 2
    id: "#md5file"
    type:
      - File
    inputBinding:
        position: 3
outputs:
  - id: "#report"
    type:
    - File
    outputBinding:
      glob: report
hints:
  - dockerPull: duplexa/md5:v2
    class: DockerRequirement
class: CommandLineTool