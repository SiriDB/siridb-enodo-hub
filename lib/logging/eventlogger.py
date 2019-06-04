import datetime


class EventLogger:
    log_file_path: None
    print_line = True
    split_char = '[\<->/]'

    log_lines = []

    @classmethod
    async def prepare(cls, path):
        cls.log_file_path = path
        cls.parse()

    @classmethod
    def log(cls, message, log_level="info", message_type="", serie=""):
        current_dt = datetime.datetime.now()
        cls.log_lines.append({
            'datetime': str(current_dt),
            'log_level': log_level,
            'message_type': message_type,
            'serie_name': serie,
            'message': message
        })

        if cls.print_line:
            log_line = f'{str(current_dt)} - {log_level} - {message_type} - {serie} - {message} \n'
            print(log_line)

    @classmethod
    def parse(cls):
        for line in list(open(cls.log_file_path)):
            line = line.rstrip()
            elements = line.split(f' {cls.split_char} ')
            cls.log_lines.append({
                'datetime': elements[0],
                'log_level': elements[1],
                'message_type': elements[2],
                'serie_name': elements[3],
                'message': elements[4]
            })

    @classmethod
    def save_to_disk(cls):
        f = open(cls.log_file_path, "w+")
        for line in cls.log_lines:
            log_line = f'{line.datetime} {cls.split_char} {line.log_lever} {cls.split_char} \
                {line.message_type} {cls.split_char} {line.serie_name} {cls.split_char} {line.message} \n'
            f.write(log_line)
        f.close()

    @classmethod
    def get(cls, serie_depended=True):
        if serie_depended:
            log_lines = []
            for line in list(reversed(cls.log_lines)):
                if line.get('serie_name'):
                    log_lines.append(line)
            return log_lines

        return list(reversed(cls.log_lines))