def json_to_csv(pre_process_obj):
    json_data = pre_process_obj.get_file_as_json()

    new_file_name = pre_process_obj.get_filename_no_extension() + ".csv"

    pre_process_obj.write_json_to_csv(new_file_name, json_data)
