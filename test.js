
var AsyncArray = require('./async-array'), assert = require('assert');


function getNumbers(n) {
	return new AsyncArray({
		current: null,
		nextBlock: function(callback) {
			var self = this;
			if (!this.current) 
				this.current = [1, 2, 3, 4];
			else 
				this.current = this.current.map(function(i) {
					return i + self.current.length;
				})
			
			var tmp = this.current.slice();
			
			process.nextTick(function() {
				var left = tmp[tmp.length-1] - n;
				if (left > 0)
					callback(tmp.slice(0, tmp.length - left))
				else
					callback(tmp)
			});
			
		}
	});
}

var sum = 0, n = 12;
for (var i = 1; i <= n; ++i)
	sum += i;

var numbers = getNumbers(n);

//assert.equal(n, numbers.length);

numbers.forEach(function(element, i) {
	assert.equal(element, i+1);
})

numbers.forEach(function(element, i) {
	assert.equal(element, i+1);
})

numbers.map(function(current) {
	return current + 1;
}, function(result) {
	result.forEach(function(element, i) {
		assert.equal(element, i+2);
	});
})


numbers.reduce(function(prev, current) {
	return prev + current;
}, function(result) {
	console.log(result)
	assert.equal(sum, result);
})

/*
numbers.reduce(function(prev, current) {
	return prev + current;
}, 10, function(result) {
	assert.equal(10+sum, result);
})
*/





