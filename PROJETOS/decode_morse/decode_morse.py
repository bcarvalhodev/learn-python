import os
import sys
import json
import datetime
import pandas as pd
from environs import Env

env = Env()
env.read_env('.secrets')

dict_morse = json.loads(env('DICT_MORSE'))
file_path = env('FILE_PATH')


def separate_words_in_morse_code(morse_code: str) -> list:
    return morse_code.split("  ")


def decipher_letters_of_the_morse_word(morse_word: str) -> str:
    word = ''
    for morse_letter in morse_word.split(" "):
        try:
            word += str(dict_morse.get(morse_letter))
        except TypeError:
            print(f"The morse letter {morse_letter} is not found into dict morse")
    return word


def append_phrase_on_csv_file(line: str) -> None:
    df = pd.DataFrame(data=[[line.rstrip(), datetime.datetime.now()]], columns=["mensagem", "datetime"])
    df.to_csv(file_path, mode="a", index=False, header=not os.path.exists(file_path))


if __name__ == '__main__':
    morse_phrase = sys.argv[1]
    phrase = ''
    list_of_morse_words = separate_words_in_morse_code(morse_code=morse_phrase)
    for i in list_of_morse_words:
        phrase += decipher_letters_of_the_morse_word(morse_word=i) + " "
    append_phrase_on_csv_file(line=phrase)
