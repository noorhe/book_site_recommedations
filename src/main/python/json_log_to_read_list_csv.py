import sys
import json

input_filename = sys.argv[1]
output_filename = sys.argv[2]
input_file = open(input_filename)
output_file = open(output_filename, 'w')
input_line = input_file.readline()
print(f"input_line = {input_line}")
while(input_line):
    print(f"input_line = {input_line}")
    json_dict = json.loads(input_line)
    type = json_dict["type"]
    if (type == "user_page_pair"):
        output_line = f"{json_dict['op']},{json_dict['data']['userId']}\n"
        print(f"output_line = {output_line}")
        output_file.write(output_line)
    input_line = input_file.readline()
input_file.close()
output_file.close()
