import pyproj

# 허용 하는 좌표계
ALLOWED_CRS = {
    "EPSG:5186",
    "EPSG:4004",
    "EPSG:4019",
    "EPSG:4326",
    "EPSG:5173",
    "EPSG:5174",
    "EPSG:5175",
    "EPSG:5176",
    "EPSG:5177",
    "EPSG:5178",
    "EPSG:5179",
    "EPSG:5180",
    "EPSG:5181",
    "EPSG:5182",
    "EPSG:5183",
    "EPSG:5184",
    "EPSG:5185",
    "EPSG:5186",
    "EPSG:5187",
    "EPSG:5188"
}
ALLOWED_CRS = sorted(ALLOWED_CRS)

# 현재 지도 좌표계
TARGET_CRS = "EPSG:5179"


def convert_coordinates(given_x: float, given_y: float, given_crs: str):
    """
    좌표계 변환 메소드
    """
    if given_crs not in ALLOWED_CRS:
        raise Exception("지원하지 않는 좌표계입니다 : {}".format(given_crs))

    transformer = pyproj.Transformer.from_crs(given_crs, TARGET_CRS, always_xy=True)
    converted_x, converted_y = transformer.transform(given_x, given_y)
    return converted_x, converted_y


if __name__ == '__main__':
    x = 197205.189
    y = 549620.285

    print(convert_coordinates(x, y, "EPSG:5175"))
