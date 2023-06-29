import os

"""
This function takes in an input folder and an output file, and combines the contents of all .out files in the folder into the output file.
Arguments:
   input_folder (str): the path to the folder containing .out files to be combined
    output_file (str): the path to the output file that will contain the combined contents
Returns:
    None
"""
def map_reduce_combiner(input_folder, output_file):
    # Open the output file in 'append' mode, so that new contents can be added without overwriting existing contents

    with open(output_file, 'a') as out_file:

        # Loop through each file in the input folder
        for filename in os.listdir(input_folder):
            # Skip files that do not end with the .out extension
            if not filename.endswith('.out'):
                continue

            # Open the file and loop through each line
            with open(os.path.join(input_folder, filename), 'r') as in_file:
                for line in in_file:
                    
                    # Write each line to the output file
                    out_file.write(line)
