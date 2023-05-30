#!/bin/bash
#
#SBATCH -p fasse # partition (queue)
#SBATCH -c 40 # number of cores
#SBATCH --mem 100GB # memory pool for all cores
#SBATCH -t 0-12:00 # time (D-HH:MM)

module load R 
Rscript final_aggregation.R