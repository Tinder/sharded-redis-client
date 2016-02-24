module.exports = RoundRobinSet;

function RoundRobinSet (arr) {
  this._current = 0;
  var newArr = arr.slice(0);
  for (var i = 0, l = newArr.length; i < l; i++) {
    newArr[i]._rrindex = i;
  }
  Object.defineProperty(this,"items",{ value: newArr });
}

RoundRobinSet.prototype.obtain = function (){
  var item = this.items[this._current];
  this._current = (this._current + 1) % this.items.length;
  return item;
};

RoundRobinSet.prototype.next = function (item){
  return this.items[(item._rrindex + 1) % this.items.length];
};
