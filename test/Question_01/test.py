from src.Question_1.driver import (
    df5,
    iphone13_purchases,
    df8,
    iphone13_customers,
    df11
)


# Question 1.2:
# Test whether the customer who bought only iphone13 is customer 4
def test_customers_who_bought_only_iphone13():

    result = df5.select(
        iphone13_purchases.customer
    ).first()[0]

    assert result == 4


# Question 1.3:
# Test whether customers 1 and 3 upgraded from iphone13 to iphone14
def test_customers_who_upgraded():

    result = df8.select(
        iphone13_customers.customer
    ).collect()

    assert [row[0] for row in result] == [1, 3]


# Question 1.4:
# Test whether customer 1 is the only customer who bought all product models
def test_customers_who_bought_all_models():

    assert df11.count() == 1
    assert df11.first()[0] == 1