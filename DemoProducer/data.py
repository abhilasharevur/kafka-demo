import faker
from faker import Faker

fake = Faker()


def get_key():
    return fake.word()


def get_registered_user():
    return {
        "customer": fake.name(),
        "product": fake.random_element(elements=("Beer", "Toy Car", "Toy Train", "Diaper")),
        "qty": fake.random_digit_not_null()
    }


if __name__ == "__main__":
    print(get_registered_user())
    print(get_key())
