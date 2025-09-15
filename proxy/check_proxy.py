FILE_PATH = "./proxy/proxies.txt"
with open(file = FILE_PATH, mode = "r") as file:
    ip_list = file.read().split("\n")

proxy_list = list(map(lambda x: "https://" + x, ip_list))

if __name__ == "__main__":
    print(proxy_list)