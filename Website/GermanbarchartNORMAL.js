//https://bl.ocks.org/d3noob/bdf28027e0ce70bd132edc64f1dd7ea4
// set the dimensions and margins of the graph


// set the ranges





// get the data
function d3normalbar() {
  console.log("Resize!")
  var margin = {
      top: 50,
      right: 10,
      bottom: 70,
      left: 80
  };

  // starts out with an empty canvas
  document.getElementById('Germanbarchart2').innerHTML=""

  //defines the width and height depending on the window size

  var width = document.getElementById('Germanbarchart2').clientWidth - margin.left - margin.right;
  var height = document.getElementById('Germanbarchart2').clientHeight - margin.top - margin.bottom;

  //defines the x and y-scale to translate values into width and height

  var xscale = d3.scaleBand()
      .range([0, width])
      .padding(0.1);
  var yscale = d3.scaleLinear()
      .range([height, 0]);

  console.log(height)
  //sets both axis to the right part of the svg canvas and the format for percentage displays

  var xaxis = d3.axisBottom(xscale);

  var yaxis = d3.axisLeft(yscale);

  var pformat = d3.format('.0%');




    // append the svg object to the body of the page
    // append a 'group' element to 'svg'
    // moves the 'group' element to the top left margin
  var GERnormalbarchart = d3.select("#Germanbarchart2").append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform",
          "translate(" + margin.left + "," + margin.top + ")");


  // now appending the title text for the graphic
  GERnormalbarchart.append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 0 - margin.left + 15)
      .attr("x", 0 - (height / 2))
      .attr("stroke", "black")
      .attr("fill", "black")
      .attr("font-size", "1.1em")
      .style("text-anchor", "middle")
      .text("Tweets");





      //these variables are needed later
    var sum = 0;
    var accuracy = 0;
    //loading the stats about the accuracy of detection
    d3.csv("botdetectSTATS.csv", function(error1, databot) {
        if (error1) throw error;
        console.log(databot)
        accuracy = +databot["0"].ACC

        console.log(accuracy)


        //loading the stats about normal tweets and bot tweets for each party
        d3.csv("partystats.csv", function(error, data) {
            if (error) throw error;

            //for each row we take the label of the party
            data.forEach(function(d) {
                d.label = d.party.slice(1, -1);
                //if the accuracy of the detection is below 0.8
                //the number of bottweets is based on the counting method
                //so tweets from accounts that tweeted more than 24 times within 24 hours
                //about the tracked hashtags are classified as bottweets, others as normal
                if (accuracy < 0.8) {
                    console.log("count")
                    d.measure = +d.normaltweetscount;
                } else {
                  //if the accuracy is above 0.8 we use the classification by the algorithm
                    console.log("ML")

                    d.measure = +d.normaltweetsML;
                }
                //lastly we use the color saved for each party

                d.color = d.color.slice(2, -1)
                console.log(d.color)


                //and then we calculate the sum of
                //normaltweets
                sum += d.measure


            });
            //now appending the axis actually as g elements
            GERnormalbarchart.append('g')
                .attr('transform', 'translate(0, ' + (height) + ')')
                .attr('class', 'x_axis');

            GERnormalbarchart.append('g')
                .attr('class', 'y_axis')
                .attr('transform', 'translate(0, ' + '0' + ')')





              // and mapping the data on to the axis
            xscale.domain(data.map(function(d) {
                return d.label;
            }));
            yscale.domain([0, d3.max(data, function(d) {
                return +d.measure;
            })]);

            // append the rectangles for the bar chart
            //and add the data to each of them
            var bars = GERnormalbarchart.selectAll(".bar")
                .data(data)
            console.log(data)
            //first removing bars that are too much

            bars
                .exit()
                .transition()
                .attr('y', height)
                .attr('height', 0)
                .remove();
            //now entering new bars to display the data

            var new_bars = bars
                .enter()
                .append("rect")
                .attr("class", "bar")
                .attr("width", xscale.bandwidth())
                .attr('y', height)

            //merging the existing bars with the already existing bars
            new_bars
                .merge(bars)
                .transition()
                //setting the size and positions of the bars and their fill
                .attr("height", function(d) {
                    return height - yscale(d.measure);
                })
                .attr("y", function(d) {
                    return yscale(d.measure);
                })
                .attr("x", function(d) {
                    return xscale(d.label);
                })
                .attr("fill", function(d) {
                    return (d.color);
                })

            //sticking the data to all texts
            var text = GERnormalbarchart.selectAll('.text')
                .data((data));

            //pushing out the unnceccessary texts
            text
                .exit()
                .transition()
                .attr('x', function(d) {
                    return xscale(d.label) + (xscale.bandwidth()) * 0.5;
                })
                .attr('y', height)
                .remove();

            //adding new text, if there is more data than texts
            var new_text = text
                .enter()
                .append('text')
                .attr('class', 'text')
                .attr('y', height)

            //merging the new text elements with the old elements
            //and display the percentage in the text
            new_text
                .merge(text)
                .transition()
                .style("text-anchor", "middle")
                .attr('x', function(d) {
                    return xscale(d.label) + (xscale.bandwidth()) * 0.5;
                })

            .attr('y', function(d) {
                    return (yscale(d.measure) - 5);
                })
                .text(function(d) {
                    return (pformat((d.measure / sum)));
                })
                .attr('fill', 'black');

            //selecting the axis for transition purpose
            GERnormalbarchart.select('.x_axis')
                .transition()
                .duration(1000)
                .call(xaxis);

            GERnormalbarchart.select('.y_axis')
                .transition()
                .duration(1000)
                .call(yaxis);

            //adding the title to the graph
            GERnormalbarchart.append("text")
                .attr("class", "x label")
                .attr("stroke", "black")
                .attr("fill", "black")
                .attr("text-anchor", "middle")
                .attr("x", width / 2)
                .attr("y", -30)
                .attr("font-size", "1.1em")
                .text("'Normal'-Tweets");


        });
    });
}
var choicedropA = 0;
console.log(choicedropA);
d3normalbar()
//resizing in case of window change
 window.addEventListener("resize",d3normalbar)
