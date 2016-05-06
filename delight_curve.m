% hold on the figure
figure;
hold on;

for flight_segement = 1:5
    flight_hour = linspace(0, 10, 100);
    flight_comfort = 50 * (1 / flight_segement) + 50 * max(0, 1 - exp(-4 + 0.5 * flight_hour));
    plot(flight_hour, flight_comfort);
end

title('Delight Index Curve');
xlabel('flight hour');
ylabel('delight index');
legend('segement = 1', 'segement = 2', 'segement = 3', 'segement = 4', 'segement = 5');