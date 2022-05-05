import sys

input_filename = sys.argv[1]
output_filename = sys.argv[2]
input_file = open(input_filename)
output_file = open(output_filename, 'w')
input_line = input_file.readline()
output_file.write("[")
output_file.write(input_line)
print(f"input_line = {input_line}")
while(input_line):
    print(f"input_line = {input_line}")
    output_file.write(",")
    output_file.write(input_line.strip())
    input_line = input_file.readline()
output_file.write("]")
input_file.close()
output_file.close()
