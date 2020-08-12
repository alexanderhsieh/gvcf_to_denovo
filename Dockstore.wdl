version 1.0

import "https://raw.githubusercontent.com/alexanderhsieh/EM-mosaic-pipeline/master/tasks/tasks_gvcf_to_denovo.wdl" as call_denovo


###########################################################################
## WORKFLOW DEFINITION
###########################################################################
workflow gvcf_to_denovo {
	input {

		File sample_table
		File sample_map
		File ped
		
		File ref_fasta
		File ref_fasta_index

		Float pb_min_vaf
		Int par_min_dp
		Int par_max_alt

		String output_prefix
		String output_suffix
	}

	## Parse sample table downloaded from Terra (.tsv)
	## Generate table that enables grouping trio gvcfs together as Array[File]
	call call_denovo.read_terra_table{
		input:
			terra_table = sample_table,
			sample_map = sample_map,
			ped = ped
	}

	## Read in table and coerce as Array[File]
	call call_denovo.read_table {
		input:
			table = read_terra_table.out
	}

	## i index (rows) correspond to individual samples
	scatter (i in range(length(read_table.out))) {

		Int n_cols = length(read_table.out[0])

		String sample_id = read_table.out[i][0]
		Array[File] selected_gvcf_columns = [ read_table.out[i][1], read_table.out[i][2], read_table.out[i][3] ]
		Array[File] selected_gvcf_index_columns = [ read_table.out[i][4], read_table.out[i][5], read_table.out[i][6] ]

		call gvcf_to_denovo.merge_trio_gvcf {
			input:
				sample_id = sample_id,
				trio_gvcf_array = selected_gvcf_columns,
				trio_gvcf_index_array = selected_gvcf_index_columns,
				ref_fasta = ref_fasta,
				ref_fasta_index = ref_fasta_index
		}

		call gvcf_to_denovo.call_denovos {
			input:
				sample_id = sample_id,
				trio_readgroup_ids = merge_trio_gvcf.rg_ids,
				gvcf = merge_trio_gvcf.out_gvcf,
				pb_min_vaf = pb_min_vaf,
				par_max_alt = par_max_alt,
				par_min_dp = par_min_dp,
				output_suffix = output_suffix
		}

	}

	call call_denovo.gather_shards {
		input:
			shards = call_denovos.out,
			headers = call_denovos.head,
			output_prefix = output_prefix,
			output_suffix = output_suffix
	}


	output {
		File raw_denovos = gather_shards.out
	}



}




