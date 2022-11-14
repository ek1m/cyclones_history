COPY (
    select
        id,
        "date",
        status
    from
    (
        select
            row_number() over(partition by c.id, c."date" order by "time" desc) as rn ,
            c.id ,
		    c."date" ,
            c.status
        from tmp.cyclones c
        where c."date" = '{date}'
    ) as cyc
    where cyc.rn = 1
) TO STDOUT {copy_params} ;