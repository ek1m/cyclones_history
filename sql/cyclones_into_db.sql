COPY tmp.cyclones_tmp (id, date, status)
FROM stdin
DELIMITER '{delimiter}' CSV ;