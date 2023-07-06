# Tibanna Scheduler
Provides additional options for upscaling Tibanna workflow requests accross AWS instances and makes estmimating costs of large distributed jobs easier

Additional functionalities include:

-parallelizing large batches of inputs both across and within instances (as opposed to relying on the limited capabilities of CWL's scatter property)

-modular design for plug and play with Docker images

-cost estimation of large batches of jobs

-easy cleanup of Tibanna logs, and input files no longer needed to save on storage costs
