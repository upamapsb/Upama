from cowidev.utils.log import get_logger
from cowidev.utils.s3 import obj_to_s3
from cowidev.utils import paths

from cowidev.vax.batch.latvia import Latvia


PATH_ICE = paths.S3.VAX_ICE
logger = get_logger()

countries = [Latvia()]


def main():
    for country in countries:
        logger.info(f"VAX - ICE - {country.location}")
        df = country.read()
        obj_to_s3(df, f"{PATH_ICE}/{country.location}.csv")


if __name__ == "__main__":
    main()
