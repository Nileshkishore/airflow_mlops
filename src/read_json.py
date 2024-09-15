import json

def print_file_contents(file_path):
    """Print the contents of a file."""
    try:
        with open(file_path, 'r') as file:
            contents = file.read()
            print(f"Contents of {file_path}:\n{contents}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

if __name__ == "__main__":
    # Other code...
    print_file_contents('best_model_and_params.json')
