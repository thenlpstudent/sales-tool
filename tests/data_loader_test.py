from asales import main


def test_file_list():
    data_loader_instance = main.DataLoader("data")
    assert len(data_loader_instance.file_list) != 0


def test_files_to_dataframe():
    data_loader_instance = main.DataLoader("data")
    assert main.DataLoader.load_data_frames_from_list(data_loader_instance.file_list) is not None
