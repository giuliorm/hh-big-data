import requests as r

def main():
    res = r.get("https://api.hh.ru/resumes/49c3257f0002ce937f0039ed1f656d706f5341")

    print(res)


main()