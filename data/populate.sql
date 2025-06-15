-- Aeroportos (Airports)
TRUNCATE TABLE bilhete CASCADE;
TRUNCATE TABLE venda CASCADE;
TRUNCATE TABLE assento CASCADE;
TRUNCATE TABLE voo CASCADE;
TRUNCATE TABLE aviao CASCADE;
TRUNCATE TABLE aeroporto CASCADE;

INSERT INTO aeroporto (codigo, nome, cidade, pais) VALUES
-- London (2 airports)
('LHR', 'Heathrow Airport', 'London', 'United Kingdom'),
('LGW', 'Gatwick Airport', 'London', 'United Kingdom'),
-- Paris (2 airports)
('CDG', 'Charles de Gaulle Airport', 'Paris', 'France'),
('ORY', 'Orly Airport', 'Paris', 'France'),
-- Other major European airports
('FRA', 'Frankfurt Airport', 'Frankfurt', 'Germany'),
('AMS', 'Amsterdam Airport Schiphol', 'Amsterdam', 'Netherlands'),
('MAD', 'Adolfo Suárez Madrid–Barajas Airport', 'Madrid', 'Spain'),
('BCN', 'Barcelona–El Prat Airport', 'Barcelona', 'Spain'),
('FCO', 'Leonardo da Vinci–Fiumicino Airport', 'Rome', 'Italy'),
('LIN', 'Milan Linate Airport', 'Milan', 'Italy'),
('IST', 'Istanbul Airport', 'Istanbul', 'Turkey'),
('ZRH', 'Zurich Airport', 'Zurich', 'Switzerland'),
('CPH', 'Copenhagen Airport', 'Copenhagen', 'Denmark');

-- Aviões (Airplanes)
INSERT INTO aviao (no_serie, modelo) VALUES
-- Airbus A320 (5 planes)
('A320-1001', 'Airbus A320'),
('A320-1002', 'Airbus A320'),
('A320-1003', 'Airbus A320'),
('A320-1004', 'Airbus A320'),
('A320-1005', 'Airbus A320'),
-- Boeing 737 (3 planes)
('B737-2001', 'Boeing 737'),
('B737-2002', 'Boeing 737'),
('B737-2003', 'Boeing 737'),
-- Embraer E190 (2 planes)
('E190-3001', 'Embraer E190'),
('E190-3002', 'Embraer E190');

-- Assentos (Seats) - creating realistic seat configurations for each plane type
-- Airbus A320 typically has 150-180 seats, let's assume 150 (15 first class, 135 economy)
DO $$
DECLARE
    plane_no VARCHAR;
    row_num INT;
    seat_letter CHAR;
BEGIN
    FOR i IN 1..5 LOOP
        plane_no := 'A320-100' || i;
        
        -- First class (rows 1-3, 5 seats per row: A,B,C,D,E)
        FOR row_num IN 1..3 LOOP
            FOREACH seat_letter IN ARRAY ARRAY['A','B','C','D','E']::CHAR[] LOOP
                INSERT INTO assento (lugar, no_serie, prim_classe) VALUES 
                (row_num || seat_letter, plane_no, TRUE);
            END LOOP;
        END LOOP;
        
        -- Economy class (rows 4-30, 6 seats per row: A,B,C,D,E,F)
        FOR row_num IN 4..30 LOOP
            FOREACH seat_letter IN ARRAY ARRAY['A','B','C','D','E','F']::CHAR[] LOOP
                INSERT INTO assento (lugar, no_serie, prim_classe) VALUES 
                (row_num || seat_letter, plane_no, FALSE);
            END LOOP;
        END LOOP;
    END LOOP;
    
    -- Boeing 737 typically has 160-200 seats, let's assume 160 (16 first class, 144 economy)
    FOR i IN 1..3 LOOP
        plane_no := 'B737-200' || i;
        
        -- First class (rows 1-4, 4 seats per row: A,B,C,D)
        FOR row_num IN 1..4 LOOP
            FOREACH seat_letter IN ARRAY ARRAY['A','B','C','D']::CHAR[] LOOP
                INSERT INTO assento (lugar, no_serie, prim_classe) VALUES 
                (row_num || seat_letter, plane_no, TRUE);
            END LOOP;
        END LOOP;
        
        -- Economy class (rows 5-36, 6 seats per row: A,B,C,D,E,F)
        FOR row_num IN 5..36 LOOP
            FOREACH seat_letter IN ARRAY ARRAY['A','B','C','D','E','F']::CHAR[] LOOP
                INSERT INTO assento (lugar, no_serie, prim_classe) VALUES 
                (row_num || seat_letter, plane_no, FALSE);
            END LOOP;
        END LOOP;
    END LOOP;
    
    -- Embraer E190 typically has 100 seats, let's assume 100 (10 first class, 90 economy)
    FOR i IN 1..2 LOOP
        plane_no := 'E190-300' || i;
        
        -- First class (rows 1-2, 5 seats per row: A,B,C,D,E)
        FOR row_num IN 1..2 LOOP
            FOREACH seat_letter IN ARRAY ARRAY['A','B','C','D','E']::CHAR[] LOOP
                INSERT INTO assento (lugar, no_serie, prim_classe) VALUES 
                (row_num || seat_letter, plane_no, TRUE);
            END LOOP;
        END LOOP;
        
        -- Economy class (rows 3-20, 5 seats per row: A,B,C,D,E)
        FOR row_num IN 3..20 LOOP
            FOREACH seat_letter IN ARRAY ARRAY['A','B','C','D','E']::CHAR[] LOOP
                INSERT INTO assento (lugar, no_serie, prim_classe) VALUES 
                (row_num || seat_letter, plane_no, FALSE);
            END LOOP;
        END LOOP;
    END LOOP;
END $$;

-- Voos (Flights) - creating flights between Jan 1 and Jul 31, 2025
-- We'll create routes between airports and then generate daily flights for each route
DO $$
DECLARE
    -- TODO adicionar as outras routes
    routes TEXT[][] := ARRAY[
        ARRAY['LHR', 'CDG'], ARRAY['CDG', 'LHR'],
        ARRAY['LHR', 'ORY'], ARRAY['ORY', 'LHR'],
        ARRAY['LHR', 'FRA'], ARRAY['FRA', 'LHR'],
        ARRAY['LHR', 'AMS'], ARRAY['AMS', 'LHR'],
        ARRAY['LHR', 'MAD'], ARRAY['MAD', 'LHR'],
        ARRAY['LHR', 'BCN'], ARRAY['BCN', 'LHR'],
        ARRAY['LHR', 'FCO'], ARRAY['FCO', 'LHR'],
        ARRAY['LHR', 'LIN'], ARRAY['LIN', 'LHR'],
        ARRAY['LHR', 'IST'], ARRAY['IST', 'LHR'],
        ARRAY['LHR', 'ZRH'], ARRAY['ZRH', 'LHR'],
        ARRAY['LHR', 'CPH'], ARRAY['CPH', 'LHR'],

        ARRAY['LGW', 'CDG'], ARRAY['CDG', 'LGW'],
        ARRAY['LGW', 'ORY'], ARRAY['ORY', 'LGW'],
        ARRAY['LGW', 'FRA'], ARRAY['FRA', 'LGW'],
        ARRAY['LGW', 'AMS'], ARRAY['AMS', 'LGW'],
        ARRAY['LGW', 'MAD'], ARRAY['MAD', 'LGW'],
        ARRAY['LGW', 'BCN'], ARRAY['BCN', 'LGW'],
        ARRAY['LGW', 'FCO'], ARRAY['FCO', 'LGW'],
        ARRAY['LGW', 'LIN'], ARRAY['LIN', 'LGW'],
        ARRAY['LGW', 'IST'], ARRAY['IST', 'LGW'],
        ARRAY['LGW', 'ZRH'], ARRAY['ZRH', 'LGW'],
        ARRAY['LGW', 'CPH'], ARRAY['CPH', 'LGW'],

        ARRAY['CDG', 'FRA'], ARRAY['FRA', 'CDG'],
        ARRAY['CDG', 'AMS'], ARRAY['AMS', 'CDG'],
        ARRAY['CDG', 'MAD'], ARRAY['MAD', 'CDG'],
        ARRAY['CDG', 'BCN'], ARRAY['BCN', 'CDG'],
        ARRAY['CDG', 'FCO'], ARRAY['FCO', 'CDG'],
        ARRAY['CDG', 'LIN'], ARRAY['LIN', 'CDG'],
        ARRAY['CDG', 'IST'], ARRAY['IST', 'CDG'],
        ARRAY['CDG', 'ZRH'], ARRAY['ZRH', 'CDG'],
        ARRAY['CDG', 'CPH'], ARRAY['CPH', 'CDG'],

        ARRAY['ORY', 'FRA'], ARRAY['FRA', 'ORY'],
        ARRAY['ORY', 'AMS'], ARRAY['AMS', 'ORY'],
        ARRAY['ORY', 'MAD'], ARRAY['MAD', 'ORY'],
        ARRAY['ORY', 'BCN'], ARRAY['BCN', 'ORY'],
        ARRAY['ORY', 'FCO'], ARRAY['FCO', 'ORY'],
        ARRAY['ORY', 'LIN'], ARRAY['LIN', 'ORY'],
        ARRAY['ORY', 'IST'], ARRAY['IST', 'ORY'],
        ARRAY['ORY', 'ZRH'], ARRAY['ZRH', 'ORY'],
        ARRAY['ORY', 'CPH'], ARRAY['CPH', 'ORY'],

        ARRAY['FRA', 'AMS'], ARRAY['AMS', 'FRA'],
        ARRAY['FRA', 'MAD'], ARRAY['MAD', 'FRA'],
        ARRAY['FRA', 'BCN'], ARRAY['BCN', 'FRA'],
        ARRAY['FRA', 'FCO'], ARRAY['FCO', 'FRA'],
        ARRAY['FRA', 'LIN'], ARRAY['LIN', 'FRA'],
        ARRAY['FRA', 'IST'], ARRAY['IST', 'FRA'],
        ARRAY['FRA', 'ZRH'], ARRAY['ZRH', 'FRA'],
        ARRAY['FRA', 'CPH'], ARRAY['CPH', 'FRA'],

        ARRAY['AMS', 'MAD'], ARRAY['MAD', 'AMS'],
        ARRAY['AMS', 'BCN'], ARRAY['BCN', 'AMS'],
        ARRAY['AMS', 'FCO'], ARRAY['FCO', 'AMS'],
        ARRAY['AMS', 'LIN'], ARRAY['LIN', 'AMS'],
        ARRAY['AMS', 'IST'], ARRAY['IST', 'AMS'],
        ARRAY['AMS', 'ZRH'], ARRAY['ZRH', 'AMS'],
        ARRAY['AMS', 'CPH'], ARRAY['CPH', 'AMS'],

        ARRAY['MAD', 'BCN'], ARRAY['BCN', 'MAD'],
        ARRAY['MAD', 'FCO'], ARRAY['FCO', 'MAD'],
        ARRAY['MAD', 'LIN'], ARRAY['LIN', 'MAD'],
        ARRAY['MAD', 'IST'], ARRAY['IST', 'MAD'],
        ARRAY['MAD', 'ZRH'], ARRAY['ZRH', 'MAD'],
        ARRAY['MAD', 'CPH'], ARRAY['CPH', 'MAD'],

        ARRAY['BCN', 'FCO'], ARRAY['FCO', 'BCN'],
        ARRAY['BCN', 'LIN'], ARRAY['LIN', 'BCN'],
        ARRAY['BCN', 'IST'], ARRAY['IST', 'BCN'],
        ARRAY['BCN', 'ZRH'], ARRAY['ZRH', 'BCN'],
        ARRAY['BCN', 'CPH'], ARRAY['CPH', 'BCN'],

        ARRAY['FCO', 'LIN'], ARRAY['LIN', 'FCO'],
        ARRAY['FCO', 'IST'], ARRAY['IST', 'FCO'],
        ARRAY['FCO', 'ZRH'], ARRAY['ZRH', 'FCO'],
        ARRAY['FCO', 'CPH'], ARRAY['CPH', 'FCO'],

        ARRAY['LIN', 'IST'], ARRAY['IST', 'LIN'],
        ARRAY['LIN', 'ZRH'], ARRAY['ZRH', 'LIN'],
        ARRAY['LIN', 'CPH'], ARRAY['CPH', 'LIN'],

        ARRAY['IST', 'ZRH'], ARRAY['ZRH', 'IST'],
        ARRAY['IST', 'CPH'], ARRAY['CPH', 'IST'],

        ARRAY['ZRH', 'CPH'], ARRAY['CPH', 'ZRH']

    ];
    
    flight_date DATE;
    departure_time TIMESTAMP;
    arrival_time TIMESTAMP;
    flight_duration INTERVAL;
    plane_index INT;
    plane_no VARCHAR;
    flight_id INT;
    route_index INT;
    plane_assignment JSONB := '{}'::JSONB;
    current_location JSONB := '{}'::JSONB;
    
    -- Get all planes
    planes TEXT[] := ARRAY(
        SELECT no_serie FROM aviao ORDER BY no_serie
    );
BEGIN
    -- Initialize current location for each plane (start at random airports)
    FOR i IN 1..array_length(planes, 1) LOOP
        current_location := jsonb_set(current_location, ARRAY[planes[i]], 
            to_jsonb((SELECT codigo FROM aeroporto ORDER BY random() LIMIT 1)));
    END LOOP;
    
    -- Generate flights for each day from Jan 1 to Jul 31, 2025
    FOR flight_date IN SELECT generate_series(
        '2025-01-01'::DATE, '2025-07-31'::DATE, '1 day'::INTERVAL
    ) LOOP
        -- Generate 5 flights per day (more on busy days)
        FOR i IN 1..5 + (CASE WHEN EXTRACT(DOW FROM flight_date) IN (0,6) THEN 3 ELSE 0 END) LOOP
            -- Select a random route
            route_index := 1 + floor(random() * array_length(routes, 1))::INT;
            
            -- Assign a plane that's currently at the departure airport
            plane_no := NULL;
            FOR j IN 1..array_length(planes, 1) LOOP
                IF (current_location->>planes[j]) = routes[route_index][1] THEN
                    plane_no := planes[j];
                    EXIT;
                END IF;
            END LOOP;
            
            -- If no plane at departure airport, pick any plane (shouldn't happen with our data)
            IF plane_no IS NULL THEN
                plane_no := planes[1 + floor(random() * array_length(planes, 1))::INT];
            END IF;
            
            -- Calculate flight times (random between 1-5 hours)
            flight_duration := (1 + random() * 4) * INTERVAL '1 hour';
            departure_time := flight_date + (8 + random() * 10) * INTERVAL '1 hour';
            arrival_time := departure_time + flight_duration;
            
            -- Insert the flight
            INSERT INTO voo (no_serie, hora_partida, hora_chegada, partida, chegada)
            VALUES (plane_no, departure_time, arrival_time, routes[route_index][1], routes[route_index][2])
            RETURNING id INTO flight_id;
            
            -- Update plane's current location
            current_location := jsonb_set(current_location, ARRAY[plane_no], 
                to_jsonb(routes[route_index][2]));
        END LOOP;
    END LOOP;
END $$;

-- Vendas (Sales) and Bilhetes (Tickets) - generating 10,000+ sales with 30,000+ tickets
DO $$
DECLARE
    sale_id INT;
    flight_ids INT[] := ARRAY(SELECT id FROM voo ORDER BY random());
    flight_index INT := 1;
    sale_count INT := 0;
    ticket_count INT := 0;
    airports TEXT[] := ARRAY(SELECT codigo FROM aeroporto);
    flight_record RECORD;
    seat_record RECORD;
    first_class_seats TEXT[];
    economy_seats TEXT[];
    -- TODO talvez adicionar mais nomes, doutro ficheiro provavel
    passenger_names TEXT[] := ARRAY[
        'James Smith', 'Maria Garcia', 'John Johnson', 'Anna Müller', 'Robert Wilson',
        'Sophia Rodriguez', 'Michael Brown', 'Emma Davis', 'David Martinez', 'Olivia Taylor',
        'Daniel Anderson', 'Isabella Thomas', 'Matthew Hernandez', 'Charlotte Moore',
        'Christopher Martin', 'Amelia Jackson', 'Andrew Thompson', 'Mia White', 'Joshua Lopez',
        'Harper Lee', 'Joseph Perez', 'Evelyn Harris', 'William Clark', 'Abigail Lewis',
        'Alexander Robinson', 'Emily Walker', 'Ryan Hall', 'Elizabeth Young', 'Nicholas Allen',
        'Sofia King'
    ];
    passenger_index INT;
    ticket_price NUMERIC;
    first_class_price NUMERIC;
    economy_price NUMERIC;
    is_first_class BOOLEAN;
    seats_available INT;
    tickets_per_sale INT;
    sale_time TIMESTAMP;
BEGIN
    -- For each flight, generate sales and tickets
    FOR flight_record IN SELECT id, no_serie FROM voo ORDER BY random() LOOP
        -- Get available seats for this flight
        first_class_seats := ARRAY(
            SELECT lugar FROM assento 
            WHERE no_serie = flight_record.no_serie AND prim_classe = TRUE
            ORDER BY random()
        );
        
        economy_seats := ARRAY(
            SELECT lugar FROM assento 
            WHERE no_serie = flight_record.no_serie AND prim_classe = FALSE
            ORDER BY random()
        );
        
        -- Set prices based on flight distance (simplified)
        first_class_price := 500 + (random() * 1000)::NUMERIC(7,2);
        economy_price := 100 + (random() * 400)::NUMERIC(7,2);
        
        -- Generate sales for this flight (between 10 and 50 sales per flight)
        FOR i IN 1..(10 + floor(random() * 40))::INT LOOP
            sale_count := sale_count + 1;
            sale_time := (SELECT hora_partida FROM voo WHERE id = flight_record.id) - 
                         (1 + random() * 30) * INTERVAL '1 day';
            
            -- Create sale
            INSERT INTO venda (nif_cliente, balcao, hora)
            VALUES (
                floor(100000000 + random() * 899999999)::CHAR(9),
                airports[1 + floor(random() * array_length(airports, 1))::INT],
                sale_time
            )
            RETURNING codigo_reserva INTO sale_id;
            
            -- Generate tickets for this sale (1-5 tickets per sale)
            tickets_per_sale := 1 + floor(random() * 4)::INT;
            FOR j IN 1..tickets_per_sale LOOP
                ticket_count := ticket_count + 1;
                
                -- Decide if first class or economy (10% chance for first class)
                is_first_class := (random() < 0.1);
                
                -- Select a passenger name
                passenger_index := 1 + floor(random() * array_length(passenger_names, 1))::INT;
                
                -- Insert ticket
                IF is_first_class AND array_length(first_class_seats, 1) > 0 THEN
                    -- First class ticket
                    INSERT INTO bilhete (
                        voo_id, codigo_reserva, nome_passegeiro, preco, prim_classe, lugar, no_serie
                    ) VALUES (
                        flight_record.id, sale_id, passenger_names[passenger_index], 
                        first_class_price, TRUE, first_class_seats[1], flight_record.no_serie
                    )
                    ON CONFLICT (voo_id, codigo_reserva, nome_passegeiro) DO NOTHING; -- Add this line
                    
                    -- Remove used seat only if insert was successful (more robust, but complex here, for now, keep as is or check affected rows)
                    IF FOUND THEN -- Check if the INSERT actually happened
                        first_class_seats := first_class_seats[2:array_length(first_class_seats, 1)];
                END IF;

                ELSIF array_length(economy_seats, 1) > 0 THEN
                    -- Economy ticket
                    INSERT INTO bilhete (
                        voo_id, codigo_reserva, nome_passegeiro, preco, prim_classe, lugar, no_serie
                    ) VALUES (
                        flight_record.id, sale_id, passenger_names[passenger_index], 
                        economy_price, FALSE, economy_seats[1], flight_record.no_serie
                    )
                    ON CONFLICT (voo_id, codigo_reserva, nome_passegeiro) DO NOTHING; -- Add this line
                    
                    IF FOUND THEN -- Check if the INSERT actually happened
                        economy_seats := economy_seats[2:array_length(economy_seats, 1)];
                    END IF;
                END IF;
            END LOOP;
            
            -- Exit if we've reached our targets
            IF sale_count >= 10000 AND ticket_count >= 30000 THEN -- Ensure this condition is what you intend
                RAISE NOTICE 'Target sale/ticket count reached. Exiting population script.';
                RETURN; -- This will exit the entire DO block
            END IF;
        END LOOP;
        
        -- Exit if we've reached our targets
        IF sale_count >= 10000 AND ticket_count >= 30000 THEN -- Ensure this condition is what you intend
            RAISE NOTICE 'Target sale/ticket count reached. Exiting population script.';
            RETURN; -- This will exit the entire DO block
        END IF;
    END LOOP;
    
    RAISE NOTICE 'Created % sales with % tickets', sale_count, ticket_count;
END $$;