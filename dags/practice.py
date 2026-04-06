numbers = [1, 2, 3, 4]
squared_numbers = []
for x in numbers:
    squared_numbers.append(x ** 6)
print(squared_numbers)

numbers = [1, 2, 3, 4, 6, 8]
numbers.remove(3)
numbers.remove(8)
numbers.extend([9,10,11,12,13])
print(numbers)

numbers.pop(2)
print(numbers)

