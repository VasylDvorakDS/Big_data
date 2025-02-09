def reducer(prices):

    avg_price = prices['price'].mean()
    variance_price = prices['price'].var(ddof=0)
    return avg_price, variance_price, len(prices['price'])
