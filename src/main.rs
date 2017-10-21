extern crate libc;
use std::net;
use std::os::unix::net::UnixStream;
use std::os::unix::io::AsRawFd;
use std::io::{Result, Error, Read, Write, ErrorKind};
use std::fs::File;
use std::vec::Vec;
use std::mem::{uninitialized, transmute, size_of};
use std::ptr;
use std::slice;
use std::thread;
use std::sync::atomic::AtomicPtr;
use std::env;

const NBD_SET_SOCK: u64 = 43776;
const NBD_SET_BLKSIZE: u64 = 43777;
const NBD_SET_SIZE: u64 = 43778;
const NBD_DO_IT: u64 = 43779;
const NBD_CLEAR_SOCK: u64 = 43780;
const NBD_CLEAR_QUE: u64 = 43781;
const NBD_PRINT_DEBUG: u64 = 43782;
const NBD_SET_SIZE_BLOCKS: u64 = 43783;
const NBD_DISCONNECT: u64 = 43784;
const NBD_SET_TIMEOUT: u64 = 43785;
const NBD_SET_FLAGS: u64 = 43786;

const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_REPLY_MAGIC: [u8; 4] = [0x67, 0x44, 0x66, 0x98];

const NBD_FLAG_CAN_MULTI_CONN: u64 = (1 << 8);

const NBD_CMD_READ: u32 = 0;
const NBD_CMD_WRITE: u32 = 1;
const NBD_CMD_DISC: u32 = 2;
const NBD_CMD_FLUSH: u32 = 3;
const NBD_CMD_TRIM: u32 = 4;

const BLOCK_SIZE: u64 = 4096;
const DEVICE_SIZE: u64 = 3 * 1024 * 1024 * 1024 / 2;

#[derive(Debug)]
#[repr(C)]
#[repr(packed)]
struct nbd_request {
    magic: u32,
    type_: u32,
    handle: [u8; 8],
    from: u64,
    len: u32,
}

/*
struct nbd_reply {
    magic: u32,
    error: u32,
    handle: [u8; 8],
}
 */

fn pipe() -> (i32, i32) {
    let mut fds = [0i32; 2];
    unsafe {
        if libc::pipe((&mut fds).as_mut_ptr()) == -1 {
            panic!();
        }
    }
    (fds[1], fds[0])
}

struct Handler {
    sock: UnixStream,
    pipe_in: i32,
    pipe_out: i32,
    p: *mut u8,
    l: usize,
}

impl Handler {
    fn new<'a>(sock: UnixStream, mem: &'a mut [u8]) -> Handler {
        let (pin, pout) = pipe();
        Handler {
            sock: sock,
            pipe_in: pin,
            pipe_out: pout,
            p: mem.as_mut_ptr(),
            l: mem.len(),
        }
    }

    fn recv_req(&mut self) -> Result<nbd_request> {
        unsafe {
            let mut req = uninitialized::<nbd_request>();
            let p: *mut u8 = transmute(&mut req);
            let buf = slice::from_raw_parts_mut(p, size_of::<nbd_request>());
            self.sock.read_exact(buf).unwrap();
            req.magic = u32::from_be(req.magic);
            req.type_ = u32::from_be(req.type_);
            req.from = u64::from_be(req.from);
            req.len = u32::from_be(req.len);
            assert!(req.magic == NBD_REQUEST_MAGIC);
            Ok(req)
        }
    }

    fn process_read(&mut self, req: nbd_request) -> Result<()> {
        let from = req.from as usize;
        let len = req.len as usize;
        self.send_header(req)?;
        if true {
            let mut written = 0;
            while written < len {
                unsafe {
                    let iov = libc::iovec {
                        iov_base: self.p.offset((from + written) as isize) as *mut libc::c_void,
                        iov_len: len - written,
                    };
                    if libc::vmsplice(self.pipe_in, &iov, 1, libc::SPLICE_F_GIFT) == -1 {
                        print!("vmsplice\n");
                        return Err(Error::last_os_error());
                    }
                    let ret = libc::splice(
                        self.pipe_out,
                        ptr::null_mut(),
                        self.sock.as_raw_fd(),
                        ptr::null_mut(),
                        len - written,
                        libc::SPLICE_F_MOVE,
                    );
                    if ret == -1 {
                        print!("splice\n");
                        return Err(Error::last_os_error());
                    };
                    written += ret as usize;
                }
            }
        } else {
            let begin = from;
            let end = begin + len;
            let s = unsafe { slice::from_raw_parts(self.p, self.l) };
            self.sock.write_all(&s[begin..end])?;
        }
        Ok(())
    }

    fn process_write(&mut self, req: nbd_request) -> Result<()> {
        let begin = req.from as usize;
        let end = begin + req.len as usize;
        let s = unsafe { slice::from_raw_parts_mut(self.p, self.l) };
        self.sock.read_exact(&mut s[begin..end])?;
        self.send_header(req)?;
        Ok(())
    }

    fn send_header(&mut self, req: nbd_request) -> Result<()> {
        self.sock.write_all(&NBD_REPLY_MAGIC)?;
        let error = [0u8; 4];
        self.sock.write_all(&error)?;
        self.sock.write_all(&req.handle)?;
        Ok(())
    }

    /// false to finish.
    fn process_req(&mut self, req: nbd_request) -> Result<bool> {
        match req.type_ {
            NBD_CMD_READ => self.process_read(req)?,
            NBD_CMD_WRITE => self.process_write(req)?,
            NBD_CMD_DISC => return Ok(false),
            NBD_CMD_FLUSH => self.send_header(req)?,
            NBD_CMD_TRIM => self.send_header(req)?,
            _ => panic!("unknown type: {}", req.type_),
        }
        Ok(true)
    }

    fn start_thread(&mut self) -> thread::JoinHandle<()> {
        let p = AtomicPtr::new(self);
        thread::spawn(move || {
            let mut h = unsafe { p.into_inner().as_mut().unwrap() };
            loop {
                let req = h.recv_req().unwrap();
                if !h.process_req(req).unwrap() {
                    break;
                }
            }
            h.sock.shutdown(net::Shutdown::Both).unwrap();
        })
    }
}

struct NBD {
    nbd: File,
    _client_socks: Vec<UnixStream>,
    handlers: Vec<Handler>,
    _mem: Vec<u8>,
}

impl NBD {
    fn init(n: usize) -> Result<NBD> {
        let f = File::open("/dev/nbd0")?;
        let fd = f.as_raw_fd();
        let mut cv = Vec::new();
        let mut hv = Vec::new();
        unsafe {
            let mut mem = Vec::with_capacity(DEVICE_SIZE as usize);
            mem.set_len(DEVICE_SIZE as usize);

            if libc::ioctl(fd, NBD_CLEAR_SOCK) == -1 {
                return Err(Error::last_os_error());
            }
            if libc::ioctl(fd, NBD_SET_FLAGS, NBD_FLAG_CAN_MULTI_CONN) == -1 {
                return Err(Error::last_os_error());
            }
            if libc::ioctl(fd, NBD_SET_BLKSIZE, BLOCK_SIZE) == -1 ||
                libc::ioctl(fd, NBD_SET_SIZE_BLOCKS, DEVICE_SIZE / BLOCK_SIZE) == -1
            {
                return Err(Error::last_os_error());
            }

            for _ in 0..n {
                let (c, s) = UnixStream::pair()?;
                if libc::ioctl(fd, NBD_SET_SOCK, c.as_raw_fd() as u64) == -1 {
                    return Err(Error::last_os_error());

                }
                cv.push(c);
                hv.push(Handler::new(s, &mut mem[..]));
            }


            Ok(NBD {
                nbd: f,
                _client_socks: cv,
                handlers: hv,
                _mem: mem,
            })
        }
    }

    fn run(&mut self) -> Result<()> {
        let jhs: Vec<_> = self.handlers.iter_mut().map(|h| h.start_thread()).collect();

        let fd = self.nbd.as_raw_fd();
        unsafe {
            if libc::ioctl(fd, NBD_DO_IT) == -1 {
                return Err(Error::last_os_error());
            }
        }

        let mut ret = Ok(());
        for r in jhs.into_iter().map(|jh| {
            jh.join().map_err(|e| {
                Error::new(ErrorKind::Other, format!("{:?}", e))
            })
        })
        {
            if r.is_err() {
                ret = r;
            }
        }
        ret
    }

    fn stop(&mut self) -> Result<()> {
        let fd = self.nbd.as_raw_fd();
        unsafe {
            if libc::ioctl(fd, NBD_DISCONNECT) == -1 {
                return Err(Error::last_os_error());
            }
        }
        Ok(())
    }
}

fn handle_signal(nbd: &mut NBD) -> Result<thread::JoinHandle<()>> {
    unsafe {
        let mut mask = uninitialized::<libc::sigset_t>();
        libc::sigemptyset(&mut mask);
        libc::sigaddset(&mut mask, libc::SIGINT);
        libc::sigaddset(&mut mask, libc::SIGTERM);
        libc::sigaddset(&mut mask, libc::SIGQUIT);
        libc::pthread_sigmask(libc::SIG_BLOCK, &mask, ptr::null_mut());
        let fd = libc::signalfd(-1, &mask, 0);
        if fd == -1 {
            return Err(Error::last_os_error());
        }
        let p = AtomicPtr::new(nbd);
        Ok(thread::spawn(move || {
            let mut buf = uninitialized::<libc::signalfd_siginfo>();
            libc::read(fd, transmute(&mut buf), size_of::<libc::signalfd_siginfo>());
            let mut nbd = p.into_inner().as_mut().unwrap();
            nbd.stop().unwrap();
        }))
    }
}

fn main() {
    let n: usize = env::args().nth(1).expect("# of threads").parse().unwrap();
    let mut nbd = NBD::init(n).unwrap();
    let h = handle_signal(&mut nbd).unwrap();
    print!("start\n");
    nbd.run().unwrap();
    h.join().unwrap();
}
