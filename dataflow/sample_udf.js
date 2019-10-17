function transform(line) {
  var values = line.split(' ');
  var obj = new Object();
  obj.a1= values[0];
  obj.a2 = values[1];
  obj.a3 = values[2];
  obj.a4 = values[3];
  obj.a5 = values[4];
  obj.a6 = values[5];
  obj.a7 = values[7];
  obj.cv = values[8];
  var jsonString = JSON.stringify(obj);
  return jsonString;
}