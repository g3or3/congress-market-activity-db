def partition(df, num_partitions):
    length = len(df)
    partitions = num_partitions
    fraction = length // partitions
    args = []
    start = 0

    for j in range(partitions):
        if not j == partitions - 1:
            end = start + fraction
            args.append(df.iloc[start:end, :])
            start = end
        else:
            args.append(df.iloc[start:, :])

    return args