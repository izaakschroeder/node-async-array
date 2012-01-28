
function PriorityQueue(size, compare) {
	if (typeof size === "function") {
		compare = size;
		size = undefined;
	}
	Array.call(this, size);
	this.compare = compare || function(a, b) {
		if (a > b) { return 1 };
		if (a < b) { return -1 };
		return 0;
	};
}
PriorityQueue.prototype = [ ];

PriorityQueue.prototype.up = function( newElt ) { 
	var 
		child = this.length,
		parent = Math.floor((child-1)/2);
	while( parent >= 0) {
		if( this.compare(this[parent], newElt) < 0 ) { 
			this[child] = this[parent];
			child  = parent;
			parent = Math.floor((child-1)/2);
		}
		else 
			break;
	}
	this[child] = newElt;
}

PriorityQueue.prototype.down = function() {
	var 
		parent = 0, 
		newElt = this[parent];
		child = 2*parent+1;

	while( child < this.length ) {
		if( child+1 < this.length )
			if( this.compare(this[child+1], this[child]) > 0 )
				child++;
		if( newElt < this[child] ) { 
			this[parent] = this[child];
			parent = child;
			child = 2*parent+1;
		}
		else 
			break;
	}
	this[parent] = newElt;
}

PriorityQueue.prototype.push = function( item ) {
	Array.prototype.push.call(this, item)
	this.up(item);
}

PriorityQueue.prototype.pop = function() {
	var result = Array.prototype.shift.call(this);
	this.down();
	return result;
}

PriorityQueue.prototype.peek = function() {
	return this[0];
}

/**
 * Window
 * A moveable window for iterating over large data sets in a fashion that
 * is very similar to JavaScript arrays and hides much of the asynchronous
 * complexity behind the scenes.
 */
function Window(parent, opts) {
	opts = opts || { };
	
	//Offset is how far into the whole record we are. Our view
	//of the data is initially from offset to offset + windowSize.
	this.offset = opts.offset || 0;
	
	//Our current position in the result set.
	this.position = 0;

	//Where our window currently starts with respect to the entire data
	//set
	this.windowPosition = -1;

	this.windowLength = 0;

	this.windowLast = 0;

	//Where our buffer currently starts with respect to the entire data
	//set
	this.bufferPosition = -1;

	//How many elements our buffer currently has in it
	this.bufferLength = 0;

	//How many records to buffer in memory
	this.bufferSize = opts.bufferSize || 10000;

	//The actual data associated with our window.
	this.data = opts.data || new Array(this.bufferSize);
	
	//How many elements our window is limited to.
	this.limit = opts.limit;

	//Where our data is actually coming from
	this.parent = parent;

	//Whether or not we have all the data in our buffer
	this.complete = false;

	this.endOfStream = false;

	this.callbacks = new PriorityQueue(function(a, b) {
		if (a.start > b.start) return -1;
		if (a.start < b.start) return 1;
		return 0;
	});
}


/**
 * The total number of elements including those outside the
 * window.
 * @see Array.length
 */
Window.prototype.__defineGetter__("length", function() {
	//If we have all the data in the buffer
	if (this.complete)
		//Return the length of that buffer
		return this.bufferLength;
	
	//If our data source has no length information
	if (typeof this.parent.length === undefined)
		//Then we also have no length information
		return undefined;

	//The length of our data view from the source is from our
	//offset to the end of the source.
	var sourceLength = this.parent.length - this.offset
	
	//If we have a limit return either the limit or the length of our
	//data source, which ever is smaller. If no limit then just return
	//the length of our data source.
	return this.limit ? Math.min(this.limit, sourceLength) : sourceLength;
})

/**
 *
 *
 *
 */
Window.prototype.__defineGetter__("isBufferable", function() {
	//Get the total number of elements in our set
	var n = this.length;
	
	//If we don't know how many total elements we have
	if (typeof n === "undefined")
		//We can't say for sure if we can buffer the entire set
		return false;
	
	//If the number of elements fits in the set, we can for sure
	//buffer it
	return n <= this.bufferSize;
});

/**
 * Walk through all *necessary* elements and do their callback
 * functions.
 *
 */
Window.prototype.processCallbacks = function() {
	var w = this;

	process.nextTick(function() {
		w.position = 0;
		function doBuffer() {
			//console.log("DERP")
			//If we don't have more work to do
			if (w.callbacks.length === 0)
				//We're done!
				return;
			
			var first = w.callbacks.peek();
			var offset = 0;

			//console.log("Moving buffer within "+first.start)
	
			w.moveBufferWithin(first.start, function() {
				while(w.callbacks.length > 0) {
					var element = w.callbacks.peek(), bufferEnd = w.bufferPosition + w.bufferLength, windowEnd = w.windowPosition + w.windowLength;

					//console.log(w.windowPosition)
					//console.log(element.start+" vs "+windowEnd)

					if (element.start < this.bufferPosition)
						throw "WTF?";
					if (element.start >= windowEnd)
						break;
					w.callbacks.pop();
					for(var j = 0, i = element.start; (typeof element.end === "undefined" || i < element.end) && i < windowEnd; ++i, ++j) {
						//console.log("CALLBACK")
						element.callback.call(undefined, w.data[(w.position + j) % w.bufferSize], i);
					}

					if (typeof element.end === "undefined" || i < element.end)
						w.callbacks.push({start: i, end: element.end, callback: element.callback, done: element.done});
					else if (element.done)
						element.done.call(i);
					offset = Math.max(offset, j);
				}
				//console.log("Adding "+offset);
				w.position += offset;
				//console.log(w.position)
				
				doBuffer();
				
			})

			if (w.endOfStream) {
				while(w.callbacks.length > 0) {
					var element = w.callbacks.pop();
					if (element.done)
						element.done.call(w.position);
				}
			}
		}

		doBuffer();
	})
	
}

/**
 *
 *
 *
 */
Window.prototype.nextBlock = function(callback) {
	var w = this;

	if (!this.endOfStream) {
		w.parent.nextBlock(function(data) {
			
			w.bufferPosition = Math.max(0, w.bufferPosition); //Hack for -1 position when first going
			if (data.length === 0)
				w.endOfStream = true;
			
			if (data.length > this.bufferSize)
				throw "Unable to continue, data returned exceeds buffer size!";

			//console.log(w.data)
			for(var i = 0; i < data.length; ++i) {
				var index = (w.windowLast + i) % w.bufferSize;
				w.data[index] = data[i];
			}
			w.windowLength = data.length;
			if (w.windowPosition >= 0)
				w.windowPosition += data.length;
			else
				w.windowPosition = 0;
			w.windowLast = w.windowPosition + w.windowLength;
			//console.log(w.data)
			//console.log("W position: "+w.position+", data len "+data.length)
			if (w.windowPosition > w.bufferPosition + w.bufferSize) {
				//console.log("UPDATING")
				//console.log(w.bufferPosition)
				w.bufferPosition = w.windowPosition % w.bufferSize;
				//console.log("Buf position: "+w.bufferPosition)
			}
			
			
			
			w.bufferLength = Math.min(w.bufferSize, w.bufferLength + data.length);
			
			//console.log("Buffer length: "+w.bufferLength+", buffer position: "+w.bufferPosition+", window position: "+w.windowPosition);
			//console.log(w.data)
			if (callback)
				callback();
			
		})
	}
	else {
		//console.log("No more")
	}
	
	
}

/**
 * Loop through every element and call a function on each. Optionally
 * call a function when all of the elements have been iterated through.
 * @param callback Function to call for each element.
 * @param done Function to call after iteration completes.
 * @see Array.forEach
 */
Window.prototype.forEach = function(callback, done) {
		
	this.callbacks.push({ start: 0, callback: callback, done: done})
	this.processCallbacks();	
	
}

/**
 * Fetch a single element from the result set including ones
 * which may be outside our window.
 *
 */
Window.prototype.get = function(i, callback) {
	this.callbacks.push({ start: i, end: i, callback: callback, done: done})
	this.processCallbacks();	
}

/**
 * Sets the position to a specified value
 *
 *
 */
Window.prototype.seek = function(i, callback) {
	
}

/**
 * Convert from an asynchronous array to a normal one. Note
 * that you need to have enough space to store all the data.
 *
 */
Window.prototype.toArray = function(callback) {
	var out = [ ]
	this.forEach(function(element, i) {
		out[i] = element;
	}, function() {
		callback(out);
	})
}

/**
 * Map all the objects to some other computed value.
 * @param f The mapping function.
 * @param callback Called when the mapping is complete.
 * @see Array.map
 */
Window.prototype.map = function(f, callback) {
	var out = [ ]
	this.forEach(function(element) {
		out.push(f(element));
	}, function() {
		callback(out);
	})
}

/**
 * Reduce all the objects by accumulating them using a given function.
 * @param accumulator The accumulation function.
 * @param initialValue The initial value to use.
 * @param callback Called when the reduction is complete.
 * @see Array.reduce
 */
Window.prototype.reduce = function(accumulator, initialValue, callback) {
	var hasInitialValue = arguments.length == 3;

	if (!hasInitialValue)
		callback = arguments[1];

	if(typeof accumulator !== "function") // ES5 : "If IsCallable(callbackfn) is false, throw a TypeError exception."  
		throw new TypeError("Accumulator is not callable"); 

	if(typeof callback !== "function") // ES5 : "If IsCallable(callbackfn) is false, throw a TypeError exception."  
		throw new TypeError("Callback is not callable"); 


	var  curr;  

	this.forEach(function(element, i) {
		if (i === 0 && !hasInitialValue)
			curr = element;
		else if (i === 0)
			curr = accumulator.call(undefined, curr, initialValue, i, this);  
		else
			curr = accumulator.call(undefined, curr, element, i, this); 
	}, function(n) {
		callback(curr, n);
	});
	
}

/**
 * Make a sub-window of this window.
 * @param start Where the new window starts relative to this one.
 * @param end Where the new window ends relative to this one.
 * @see Array.slice
 */
Window.prototype.slice = function(start, end) {
	return new Window(this, {offset: start, limit: end - start });
}

/**
 * Limit the size of the window to a given amount. This is 
 * equivalent to slicing the window
 *
 */
Window.prototype.limit = function(limit) {
	return new Window(this, {limit: limit });
}

/**
 * Offset the window by a given amount. This is equivalent to 
 * slicing the window.
 *
 */
Window.prototype.offset = function(offset) {
	return new Window(this, {offset: offset });
}

Window.prototype.inBuffer = function(i) {
	return (i >= this.windowPosition) && (i < (this.windowLast));
}

/**
 * Move the window so that a given entry falls within it.
 * @param i The index the window should be covering.
 *
 */
Window.prototype.moveBufferWithin = function(i, callback) {
	var w = this;

	//Moving window forward
	if (i > this.windowPosition) {
		//console.log("Moving forward to "+i+"...")
		function moveForward() {
			if (!w.inBuffer(i)) {
				//console.log("NOT IN BUF")
				w.nextBlock(function() {
					callback.call(w)
				})
			}
			else {
				//console.log("IN BUF")
				callback.call(w);
			}
			
		}
		moveForward();
	}
	//Moving window backward
	else if (i < this.windowPosition) {
		throw "Can't go backwards!"
	}
	else {
		callback.call(w);
	}
}

module.exports = Window;

